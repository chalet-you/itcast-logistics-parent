package cn.itcast.logistics.generate;

import cn.itcast.logistics.common.utils.DBHelper;
import cn.itcast.logistics.common.utils.RDBTool;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

/**
 * 物流系统Logistics业务数据库Oracle造数程序
 */
public class MockLogisticsEsDataApp {

    private static String DAT_SUFFIX = ".csv";
    private static String ENCODING = "UTF-8";
    /** Oracle **/
    private static final String ORACLE_DDL_SQL_FILE = "/oracle-db.sql";
    // 数据库表的名称
    private static List<String> oracleTableNames = Arrays.asList("tbl_waybill","tbl_express_bill");

    private static String ORACLE_USER_TABLES_SQL_KEY = "ORACLE_USER_TABLES_SQL";
    private static String ORACLE_TABLE_DDL_SQL_KEY = "ORACLE_TABLE_DDL_SQL";
    private static String ORACLE_TABLE_SCHEMA_SQL_KEY = "ORACLE_TABLE_SCHEMA_SQL";
    private static Map<String, String> oracleSqlHelps = new HashMap<String, String>() {{
        put(ORACLE_USER_TABLES_SQL_KEY, "SELECT TABLE_NAME,TABLESPACE_NAME FROM user_tables WHERE TABLESPACE_NAME='TBS_LOGISTICS'");
        put(ORACLE_TABLE_DDL_SQL_KEY, "SELECT dbms_metadata.get_ddl('TABLE',?) FROM DUAL");
        put(ORACLE_TABLE_SCHEMA_SQL_KEY, "SELECT COLUMN_NAME,DATA_TYPE FROM user_tab_columns WHERE TABLE_NAME=?");
    }};
    /** PUBLIC SQL **/
    private static String CLEAN_TABLE_SQL = "TRUNCATE TABLE ?";
    // Oracle JDBC
    private static final DBHelper oracleHelper = DBHelper.builder()
            .withDialect(DBHelper.Dialect.Oracle)
            .withUrl("jdbc:oracle:thin:@//192.168.88.10:1521/ORCL")
            .withUser("itcast")
            .withPassword("itcast")
            .build();

    public static void main( String[] args) {
        boolean isClean = true;
        if (args.length == 1 && args[0].matches("(true|false)")) {
            isClean = Boolean.parseBoolean(args[0]);
        }
        Map<String, List<String>> oracleSqls = buildOracleTableDML();
        /** ==== 初始化Oracle ==== **/
        Connection connection1 = oracleHelper.getConnection();
        // 清空表
        if(isClean) {
            // 清空Oracle表
            oracleTableNames.forEach(tableName -> {
                try {
                    System.out.println("==== 开始清空Oracle的：" + tableName + " 数据 ====");
                    RDBTool.update(CLEAN_TABLE_SQL, tableName, (sql, table) -> executeUpdate(connection1, sql.replaceAll("\\?", "\"" + table + "\""), 1));
                    System.out.println("==== 完成清空Oracle的：" + tableName + " 数据 ====");
                    Thread.sleep(200 * 2);
                } catch (Exception e) {}
            });
        }
        // 插入数据到Oracle表（每2秒插入一条记录）
        oracleSqls.forEach((table,sqlList) -> {
            try {
                System.out.println("==== 开始插入数据到Oracle的：" + table + " ====");
                sqlList.forEach(sql -> RDBTool.save(sql, sqlStr -> executeUpdate(connection1, sql, 1)));
                System.out.println("==== 完成插入数据到Oracle的：" + table + " ====");
                Thread.sleep(1000 * 2);
            } catch (Exception e) {}
        });
        oracleHelper.close(connection1);
    }

    /**
     * 根据table读取csv，并从csv文件中拼接表的INSERT语句
     */
    private static Map<String, List<String>> buildOracleTableDML() {
        Map<String, List<String>> sqls = new LinkedHashMap<>();
        // 从遍历表中获取表对应的数据文件
        oracleTableNames.forEach(table -> {
            String tableDatPath = null;
            try {
                // 根据表名获取类路径下的"表名.csv"绝对路径
                tableDatPath = MockLogisticsEsDataApp.class.getResource("/" + table + DAT_SUFFIX).getPath();
            } catch (Exception e) { }
            if(!StringUtils.isEmpty(tableDatPath)) {
                StringBuilder insertSQL = new StringBuilder();
                try {
                    // 读取"表名.csv"的数据
                    List<String> datas = IOUtils.readLines(new BOMInputStream(new FileInputStream(tableDatPath)), ENCODING);
                    // 取出首行的schema
                    String schemaStr = datas.get(0).replaceAll("\"","");
                    String[] fieldNames = schemaStr.split(",");
                    // 获取数据库中的schema定义
                    Map<String, String> schemaMap = getOracleTableSchema(table);
                    datas.remove(0);
                    List<String> tblSqls = new LinkedList<>();
                    datas.forEach(line->{
                        boolean chk = false;
                        insertSQL.append("INSERT INTO \"" + table + "\"(\"").append(schemaStr.replaceAll(",","\",\"")).append("\") VALUES(");
                        String[] vals = line.split(",");
                        for (int i = 0; i < vals.length; i++) {
                            String fieldName = fieldNames[i];
                            String type = schemaMap.get(fieldName);
                            String val = vals[i].trim();
                            if("NVARCHAR2".equals(type)) {insertSQL.append((StringUtils.isEmpty(val)?"NULL":"'"+val+"'")+",");}
                            else if("DATE".equals(type)) {insertSQL.append("to_date('"+val+"','yyyy-mm-dd hh24:mi:ss'),");}
                            else if("NUMBER".equals(type)) {insertSQL.append(""+(StringUtils.isEmpty(val)?"0":val)+",");}
                            else {insertSQL.append(val+",");}
                            int diff = 0;
                            if (i==vals.length-1&&fieldNames.length>vals.length) {
                                diff = fieldNames.length-vals.length;
                                for (int j = 0; j < diff; j++) {insertSQL.append("NULL,");}
                            }
                            chk = vals.length+diff == fieldNames.length;
                        }
                        insertSQL.setLength(insertSQL.length()-1);
                        insertSQL.append(")");
                        if(chk) {
                            tblSqls.add(insertSQL.toString());
                        }
                        insertSQL.setLength(0);
                    });
                    sqls.put(table, tblSqls);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        return sqls;
    }

    /**
     * 根据table和fields获取schema
     * @param table     表名
     */
    private static Map<String, String> getOracleTableSchema(String table) {
        Map<String, LinkedHashMap<String,String>> tableSchema = getOracleAllTableSchema(null);
        return tableSchema.get(table);
    }

    /**
     * 从项目的DDL文件中获取每一张表的schema信息<table,[<fieldName><fieldType>]>
     */
    private static Map<String, LinkedHashMap<String,String>> getOracleAllTableSchema(String path) {
        if(StringUtils.isEmpty(path)) {
            path = MockLogisticsEsDataApp.class.getResource(ORACLE_DDL_SQL_FILE).getPath();
        }
        Map<String, LinkedHashMap<String,String>> tableSchema = new LinkedHashMap<>();
        try {
            List<String> ddlSQL = FileUtils.readLines(new File(path), ENCODING);
            String table = null;
            String curLine = null;
            Map<String, String> schema = new LinkedHashMap<>();
            for (int i=0; i<ddlSQL.size(); i++) {
                curLine = ddlSQL.get(i);
                if(StringUtils.isEmpty(curLine)) {
                    continue;
                }
                if (curLine.contains("CREATE TABLE ")) {
                    table = curLine.substring(13, curLine.lastIndexOf(" ")).replaceAll("\"","");
                    continue;
                }
                if (curLine.contains("PRIMARY KEY")) {
                    LinkedHashMap<String, String> _schema = new LinkedHashMap<>();
                    _schema.putAll(schema);
                    tableSchema.put(table, _schema);
                    table = null;
                    schema.clear();
                }
                if (!StringUtils.isEmpty(table)) {
                    int offset = curLine.indexOf("(");
                    if (offset==-1) {offset = curLine.indexOf(",");}
                    String fieldInfo = curLine.substring(0, offset);
                    if(!StringUtils.isEmpty(fieldInfo)) {
                        String[] arr = fieldInfo.replaceAll("\"","").split(" ",2);
                        String fieldName = arr[0].trim();
                        String fieldType = arr[1].trim();
                        schema.put(fieldName, fieldType);
                    }
                }
            }
        } catch (IOException e) {
        }
        return tableSchema;
    }


    /**
     * 执行增删改的SQL
     */
    private static void executeUpdate(Connection connection, String sql, int dbType) {
        Statement st = null;
        ResultSet rs = null;
        int state = 0;
        try {
            if (null==connection||connection.isClosed()) {
                if(dbType==1){
                    connection = oracleHelper.getConnection();
                }
            }
            connection.setAutoCommit(false);
            st = connection.createStatement();
            state = st.executeUpdate(sql);
            if(sql.startsWith("INSERT")) {
                if (state > 0) {
                    connection.commit();
                    System.out.println("==== SQL执行成功："+sql+" ====");
                } else {
                    System.out.println("==== SQL执行失败："+sql+" ====");
                }
            } else {
                System.out.println("==== SQL执行成功："+sql+" ====");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if(dbType==1){
                oracleHelper.close(rs, st, null);
            }
        }
    }

}

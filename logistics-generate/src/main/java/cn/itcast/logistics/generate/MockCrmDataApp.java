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
 * CRM客户管理系统数据库MySQL造数程序
 */
public class MockCrmDataApp {

    private static String DAT_SUFFIX = ".csv";
    private static String ENCODING = "UTF-8";
    /** MySQL **/
    private static final String MYSQL_DDL_SQL_FILE = "/mysql-db.sql";
    private static List<String> mysqlTableNames = Arrays.asList("crm_address", "crm_customer", "crm_consumer_address_map");
    /** PUBLIC SQL **/
    private static String CLEAN_TABLE_SQL = "TRUNCATE TABLE ?";
    // MysQL JDBC
    private static final DBHelper mysqlHelper = DBHelper.builder()
            .withDialect(DBHelper.Dialect.MySQL)
            .withDriver("com.mysql.jdbc.Driver")
            .withUrl("jdbc:mysql://192.168.88.10:3306/crm?useUnicode=true&characterEncoding=utf8&autoReconnect=true&failOverReadOnly=false")
            .withUser("root")
            .withPassword("123456")
            .build();

    public static void main( String[] args) {
        boolean isClean = true;
        if (args.length == 1 && args[0].matches("(true|false)")) {
            isClean = Boolean.parseBoolean(args[0]);
        }
        Map<String, List<String>> mysqlSqls = buildMySQLTableDML();
        /** ==== 初始化MySQL ==== **/
        Connection connection2 = mysqlHelper.getConnection();
        // 清空表
        if(isClean) {
            // 清空MySQL表
            mysqlTableNames.forEach(tableName -> {
                try {
                    System.out.println("==== 开始清空MySQL的："+tableName+" 数据 ====");
                    RDBTool.update(CLEAN_TABLE_SQL, tableName, (sql, table) -> executeUpdate(connection2, sql.replaceAll("\\?", table), 0));
                    System.out.println("==== 开始清空MySQL的："+tableName+" 数据 ====");
                    Thread.sleep(200*2);
                } catch (Exception e) {}
            });
        }

        // 插入数据到MySQL表（每2秒插入一条记录）
        mysqlSqls.forEach((table,sqlList) -> {
            try {
                System.out.println("==== 开始插入数据到MySQL的："+table+" ====");
                sqlList.forEach(sql -> RDBTool.save(sql, sqlStr -> executeUpdate(connection2, sql, 0)));
                System.out.println("==== 完成插入数据到MySQL的："+table+" ====");
                Thread.sleep(1000*20);
            } catch (Exception e) {}
        });

        mysqlHelper.close(connection2);
    }

    /**
     * 根据table读取csv，并从csv文件中拼接表的INSERT语句
     */
    private static Map<String, List<String>> buildMySQLTableDML() {
        Map<String, List<String>> sqls = new LinkedHashMap<>();
        // 从遍历表中获取表对应的数据文件
        mysqlTableNames.forEach(table->{
            String tableDatPath = null;
            try {
                // 根据表名获取类路径下的"表名.csv"绝对路径
                tableDatPath = MockCrmDataApp.class.getResource("/" + table.replace("crm","tbl") + DAT_SUFFIX).getPath();
            } catch (Exception e) {
            }
            if(!StringUtils.isEmpty(tableDatPath)) {
                StringBuilder insertSQL = new StringBuilder();
                try {
                    // 读取"表名.csv"的数据
                    List<String> datas = IOUtils.readLines(new BOMInputStream(new FileInputStream(tableDatPath)), ENCODING);
                    // 取出首行的schema
                    String schemaStr = datas.get(0).replaceAll("\"","");
                    String[] fieldNames = schemaStr.split(",");
                    // 获取数据库中的schema定义
                    Map<String, String> schemaMap = getMySQLTableSchema(table);
                    datas.remove(0);
                    List<String> tblSqls = new LinkedList<>();
                    datas.forEach(line->{
                        boolean chk = false;
                        insertSQL.append("INSERT INTO " + table + "(").append(schemaStr).append(") VALUES(");
                        String[] vals = line.split(",");
                        for (int i = 0; i < vals.length; i++) {
                            String fieldName = fieldNames[i];
                            String type = schemaMap.get(fieldName);
                            String val = vals[i].trim();
                            if("INT".equals(type.toUpperCase())) {insertSQL.append(""+(StringUtils.isEmpty(val)?"0":val)+",");}
                            else if("BIGINT".equals(type.toUpperCase())) {insertSQL.append(""+(StringUtils.isEmpty(val)?"0":val)+",");}
                            else {insertSQL.append((StringUtils.isEmpty(val)?"NULL":"'"+val+"'")+",");}
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
                } catch (IOException e) {
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
    private static Map<String, String> getMySQLTableSchema(String table) {
        Map<String, LinkedHashMap<String,String>> tableSchema = getMySQLAllTableSchema(null);
        return tableSchema.get(table);
    }

    /**
     * 从项目的DDL文件中获取每一张表的schema信息<table,[<fieldName><fieldType>]>
     * @param path
     * @return
     */
    private static Map<String, LinkedHashMap<String,String>> getMySQLAllTableSchema(String path) {
        if(StringUtils.isEmpty(path)) {
            path = MockCrmDataApp.class.getResource(MYSQL_DDL_SQL_FILE).getPath();
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
                } else {
                    curLine = curLine.trim();
                }
                if (curLine.contains("CREATE TABLE ")) {
                    table = curLine.substring(13, curLine.lastIndexOf(" ")).replaceAll("`","");
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
                    if (offset==-1) {offset = curLine.indexOf("DEFAULT");}
                    if (offset==-1) {offset = curLine.indexOf("NOT");}
                    String fieldInfo = curLine.substring(0, offset);
                    if(!StringUtils.isEmpty(fieldInfo)) {
                        String[] arr = fieldInfo.trim().replaceAll("`","").split(" ",2);
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
                if(dbType==0) {
                    connection = mysqlHelper.getConnection();
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
            if(dbType==0) {
                mysqlHelper.close(rs, st, null);
            }
        }
    }

}

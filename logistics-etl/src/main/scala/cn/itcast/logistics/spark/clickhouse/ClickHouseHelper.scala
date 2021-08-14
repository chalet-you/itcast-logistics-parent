package cn.itcast.logistics.spark.clickhouse

import java.sql.ResultSet
import java.util.Date

import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import ru.yandex.clickhouse.response.{ClickHouseResultSet, ClickHouseResultSetMetaData}
import ru.yandex.clickhouse.settings.ClickHouseProperties
import ru.yandex.clickhouse.{ClickHouseConnection, ClickHouseDataSource, ClickHouseStatement}

import scala.collection.mutable.ListBuffer

/**
 * 定义 ClickHouse 表操作的工具类
 * 以JDBC的方式操作 ClickHouse表，比如DDL操作（创建删除表）和DML操作（数据CRUD）等
 */
class ClickHouseHelper(options: ClickHouseOptions) {
	
	/**
	 * 创建 ClickHouse 连接对象，获取到数据源的连接
	 */
	def getConnection: ClickHouseConnection = {
		// a. 创建 ClickHouseDataSource 连接对象
		val clickHouseDataSource: ClickHouseDataSource = new ClickHouseDataSource(
			options.url, new ClickHouseProperties()
		)
		// b. 获取ClickHouseConnection 连接对象并返回
		clickHouseDataSource.getConnection(options.user, options.password)
	}
	
	/**
	 * 返回指定表的schema信息（DataFrame的schema就是StructType）
	 */
	def getSparkSQLSchema: StructType = {
		// a. 依据表名获取到该表的所有字段名称和字段类型
		val clickHouseTableSchema: List[(String, String)] = getClickHouseTableSchema
		// b. 将ClickHouse表的字段类型转换成SparkSQL的字段类型
		val fields: List[StructField] = clickHouseTableSchema.map { case (fieldName, fieldType) =>
			StructField(fieldName, convertSparkSqlType(fieldType))
		}
		// c. 创建StructType对象并返回
		StructType(fields)
	}
	
	/**
	 * 返回获取到表结构信息，指定表的table名称
	 */
	def getClickHouseTableSchema: List[(String, String)] = {
		// 定义要返回的字段集合，存储列名和字段类型
		val fields: ListBuffer[(String, String)] = ListBuffer[(String, String)]()
		
		// 定义变量
		var conn: ClickHouseConnection = null
		var stmt: ClickHouseStatement = null
		var result: ClickHouseResultSet = null
		try {
			// a. 实例化对象
			conn = getConnection
			stmt = conn.createStatement()
			
			// b. 定义查询SQL语句，为了获取表的结果信息，不关心表的数据，因此查询的时候可以不返回表结果
			val sql: String = s"select * from ${options.table} where 1=0"
			// 执行查询，将返回的结果ResultSet -> ClickHouseResultSet
			result = stmt.executeQuery(sql).asInstanceOf[ClickHouseResultSet]
			// c. 获取到查询的指定表的元数据信息
			val metaData: ClickHouseResultSetMetaData = result.getMetaData.asInstanceOf[ClickHouseResultSetMetaData]
			// d. 获取到元数据中列的数量
			val columnCount: Int = metaData.getColumnCount
			for (index <- 1 to columnCount) {
				// 获取列名称
				val columnName: String = metaData.getColumnName(index)
				// 获取列类型
				val columnType: String = metaData.getColumnTypeName(index)
				// 将字段名称和字段类型组合为 二元组，添加到列表
				fields += columnName -> columnType
			}
		} catch {
			case ex: Exception => ex.printStackTrace()
		} finally {
			if (result != null) result.close()
			if (stmt != null) stmt.close()
			if (conn != null) conn.close()
		}
		
		// 返回列表
		fields.toList
	}
	
	/**
	 * 根据 ClickHouse 表的字段类型转换成 SparkSQL Schema 对象的字段类型
	 * IntervalYear      (Types.INTEGER,   Integer.class,    true,  19,  0),
	 * IntervalQuarter   (Types.INTEGER,   Integer.class,    true,  19,  0),
	 * IntervalMonth     (Types.INTEGER,   Integer.class,    true,  19,  0),
	 * IntervalWeek      (Types.INTEGER,   Integer.class,    true,  19,  0),
	 * IntervalDay       (Types.INTEGER,   Integer.class,    true,  19,  0),
	 * IntervalHour      (Types.INTEGER,   Integer.class,    true,  19,  0),
	 * IntervalMinute    (Types.INTEGER,   Integer.class,    true,  19,  0),
	 * IntervalSecond    (Types.INTEGER,   Integer.class,    true,  19,  0),
	 * UInt64            (Types.BIGINT,    BigInteger.class, false, 19,  0),
	 * UInt32            (Types.INTEGER,   Long.class,       false, 10,  0),
	 * UInt16            (Types.SMALLINT,  Integer.class,    false,  5,  0),
	 * UInt8             (Types.TINYINT,   Integer.class,    false,  3,  0),
	 * Int64             (Types.BIGINT,    Long.class,       true,  20,  0, "BIGINT"),
	 * Int32             (Types.INTEGER,   Integer.class,    true,  11,  0, "INTEGER", "INT"),
	 * Int16             (Types.SMALLINT,  Integer.class,    true,   6,  0, "SMALLINT"),
	 * Int8              (Types.TINYINT,   Integer.class,    true,   4,  0, "TINYINT"),
	 * Date              (Types.DATE,      Date.class,       false, 10,  0),
	 * DateTime          (Types.TIMESTAMP, Timestamp.class,  false, 19,  0, "TIMESTAMP"),
	 * Enum8             (Types.VARCHAR,   String.class,     false,  0,  0),
	 * Enum16            (Types.VARCHAR,   String.class,     false,  0,  0),
	 * Float32           (Types.FLOAT,     Float.class,      true,   8,  8, "FLOAT"),
	 * Float64           (Types.DOUBLE,    Double.class,     true,  17, 17, "DOUBLE"),
	 * Decimal32         (Types.DECIMAL,   BigDecimal.class, true,   9,  9),
	 * Decimal64         (Types.DECIMAL,   BigDecimal.class, true,  18, 18),
	 * Decimal128        (Types.DECIMAL,   BigDecimal.class, true,  38, 38),
	 * Decimal           (Types.DECIMAL,   BigDecimal.class, true,   0,  0, "DEC"),
	 * UUID              (Types.OTHER,     UUID.class,       false, 36,  0),
	 * String            (Types.VARCHAR,   String.class,     false,  0,  0, "LONGBLOB", "MEDIUMBLOB", "TINYBLOB", "MEDIUMTEXT", "CHAR", "VARCHAR", "TEXT", "TINYTEXT", "LONGTEXT", "BLOB"),
	 * FixedString       (Types.CHAR,      String.class,     false, -1,  0, "BINARY"),
	 * Nothing           (Types.NULL,      Object.class,     false,  0,  0),
	 * Nested            (Types.STRUCT,    String.class,     false,  0,  0),
	 * Tuple             (Types.OTHER,     String.class,     false,  0,  0),
	 * Array             (Types.ARRAY,     Array.class,      false,  0,  0),
	 * AggregateFunction (Types.OTHER,     String.class,     false,  0,  0),
	 * Unknown           (Types.OTHER,     String.class,     false,  0,  0);
	 *
	 */
	private def convertSparkSqlType(dataType: String) = dataType match {
		case "IntervalYear" => DataTypes.IntegerType
		case "IntervalQuarter" => DataTypes.IntegerType
		case "IntervalMonth" => DataTypes.IntegerType
		case "IntervalWeek" => DataTypes.IntegerType
		case "IntervalDay" => DataTypes.IntegerType
		case "IntervalHour" => DataTypes.IntegerType
		case "IntervalMinute" => DataTypes.IntegerType
		case "IntervalSecond" => DataTypes.IntegerType
		case "UInt64" => DataTypes.LongType
		case "UInt32" => DataTypes.LongType
		case "UInt16" => DataTypes.IntegerType
		case "UInt8" => DataTypes.IntegerType
		case "Int64" => DataTypes.LongType
		case "Int32" => DataTypes.IntegerType
		case "Int16" => DataTypes.IntegerType
		case "Int8" => DataTypes.IntegerType
		case "Date" => DataTypes.DateType
		case "DateTime" => DataTypes.TimestampType
		case "Enum8" => DataTypes.StringType
		case "Enum16" => DataTypes.StringType
		case "Float32" => DataTypes.FloatType
		case "Float64" => DataTypes.DoubleType
		case "Decimal32" => DataTypes.createDecimalType()
		case "Decimal64" => DataTypes.createDecimalType()
		case "Decimal128" => DataTypes.createDecimalType()
		case "Decimal" => DataTypes.createDecimalType()
		case "UUID" => DataTypes.StringType
		case "String" => DataTypes.StringType
		case "FixedString" => DataTypes.StringType
		case "Nothing" => DataTypes.NullType
		case "Nested" => DataTypes.StringType
		case "Tuple" => DataTypes.StringType
		case "Array" => DataTypes.StringType
		case "AggregateFunction" => DataTypes.StringType
		case "Unknown" => DataTypes.StringType
		case _ => DataTypes.NullType
	}
	
	/**
	 * 将SparkSQL字段类型转换成ClickHouse建表的字段类型
	 */
	private def convertClickHouseSqlType(sparkDataType: DataType): String = sparkDataType match {
		case DataTypes.ByteType => "Int8"
		case DataTypes.ShortType => "Int16"
		case DataTypes.IntegerType => "Int32"
		case DataTypes.FloatType => "Float32"
		case DataTypes.DoubleType => "Float64"
		case DataTypes.LongType => "Int64"
		case DataTypes.DateType => "DateTime"
		case DataTypes.TimestampType => "DateTime"
		case DataTypes.StringType => "String"
		case DataTypes.NullType => "String"
	}
	
	/**
	 * 根据列名的集合对象生成查询操作的sql语句
	 */
	def getSelectStatementSQL(schema: StructType): String = {
		//返回需要查询的表的sql语句
		s"SELECT ${schema.fieldNames.mkString(", ")} FROM ${options.table}"
	}
	
	/**
	 * 生成建表DDL语句
	 */
	def createTableDdl(schema: StructType): String = {
		// 获取数据操作类型字段名称
		val operateField: String = options.getOperateField
		
		// 第一步、遍历Schema中字段，获取列名称和数据类型
		val columns: Array[String] = for (field <- schema.fields if ! operateField.equals(field.name)) yield {
			// 获取列名称
			val fieldName: String = field.name
			// 获取列类型
			val fieldDataType: DataType = field.dataType
			// 将StructField的字段类型转换成ClickHouse的字段类型
			val clickHouseDataType: String = convertClickHouseSqlType(fieldDataType)
			// 组装列名称和数据类型
			s"${fieldName} ${clickHouseDataType}"
		}
		
		// 第二步、生成建表DDL语句
		s"""
		   |CREATE TABLE IF NOT EXISTS ${options.table}(
		   |${columns.mkString(", ")},
		   |sign Int8,
		   |version UInt32
		   |)
		   |ENGINE = VersionedCollapsingMergeTree(sign, version)
		   |ORDER BY ${options.getPrimaryKey}
		   |""".stripMargin
	}
	
	/**
	 * 根据SQL语句或DDL语句，完成对 ClickHouse 相关操作
	 */
	def executeUpdate(sql: String): Unit = {
		var conn: ClickHouseConnection = null
		var stmt: ClickHouseStatement = null
		try {
			conn = getConnection
			stmt = conn.createStatement()
			
			//执行更新操作
			stmt.executeUpdate(sql)
		} catch {
			case ex: Exception => ex.printStackTrace()
		} finally {
			if (stmt != null) stmt.close()
			if (conn != null) conn.close()
		}
	}
	
	private def getFieldValue(fieldName: String, schema: StructType, row: InternalRow): String = {
		// 依据列名称获取列索引下标
		val fieldIndex: Int = schema.fieldIndex(fieldName)
		// 依据列索引获取列数据类型
		val dataType: DataType = schema.fields(fieldIndex).dataType
		// 依据数据类型获取列的值
		val fieldValue: String = if(row.isNullAt(fieldIndex)){
			"NULL"
		}else{
			dataType match {
				case BooleanType => row.getBoolean(fieldIndex).toString
				case DoubleType => row.getDouble(fieldIndex).toString
				case FloatType => row.getFloat(fieldIndex).toString
				case IntegerType => row.getInt(fieldIndex).toString
				case LongType => row.getLong(fieldIndex).toString
				case ShortType => row.getShort(fieldIndex).toString
				case StringType => row.getString(fieldIndex).trim
				case DateType =>
					val value: Date = row.get(fieldIndex, DateType).asInstanceOf[Date]
					val dateValue: Date = new Date(value.getTime / 1000)
					FastDateFormat.getInstance("yyyy-MM-dd").format(dateValue)
				case TimestampType =>
					val value: Long = row.getLong(fieldIndex)
					val dateValue: Date = new Date(value / 1000)
					FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").format(dateValue)
				case BinaryType => row.getBinary(fieldIndex).toString
				case NullType => "NULL"
			}
		}
		// 返回获取的值
		fieldValue
	}
	
	/**
	 * 根据指定数据的操作类型生成不同的操作sql语句
	 * insert: 插入
	 * update：修改
	 * delete：删除
	 */
	def createStatementSQL(schema: StructType, row: InternalRow): String = {
		// a. 获取操作类型字段名称，options参数传递
		val operateField: String = options.getOperateField
		// b. 提取操作类型值，要么是insert、要么是update、要么是delete
		val operateTypeValue: String = getFieldValue(operateField, schema, row)
		// c. 依据操作类型生成相应的SQL语句
		operateTypeValue.toLowerCase match {
			// 如果当前数据操作类型是insert操作， 生成insert的 SQL 语句
			case "insert" => createInsertSQL(schema, row)
			// 如果当前数据操作类型是update操作， 生成update的 SQL 语句
			case "update" => createUpdateSQL(schema, row)
			// 如果当前数据操作类型是delete操作， 生成delete的 SQL 语句
			case "delete" => createDeleteSQL(schema, row)
			// 没有匹配操作类型，返回空字符串
			case _ => StringUtils.EMPTY
		}
	}
	
	/**
	 * 依据SparkSQL数据类型，判断是否对字符串左右加上单引号
	 */
	def convertValue(value: String, dataType: DataType): String = {
		if (dataType == StringType || dataType == DateType || dataType == TimestampType) {
			s"'${value}'"
		}else{
			value
		}
	}
	
	/**
	 * 生成插入操作的SQL语句
	 */
	def createInsertSQL(schema: StructType, row: InternalRow): String = {
		/*
			insert into table(....) values(?, ?, ?, ...)
			insert into table(name, ...) values('zhangsan', ....)
		 */
		// 获取操作类型字段列名称:
		val operateFieldName: String = options.getOperateField
		// 获取列名称
		val columns: String = schema.names
			.filter(name => ! operateFieldName.equals(name)) // 过滤掉操作类型字段列名称
			.mkString(", ")
		
		val values: Array[String] = for (field <- schema.fields if ! operateFieldName.equals(field.name)) yield {
			// 获取类型
			var fieldValue: String = getFieldValue(field.name, schema, row)
			// 获取列值
			val fieldDataType: DataType = field.dataType
			// 判断数据类型市是否是String、Date和DateTime，如果是值使用 字符串表示
			fieldValue = convertValue(fieldValue, fieldDataType)
			// 返回列值
			fieldValue
		}
		
		// 构建INSERT 语句
		val version: Long = System.currentTimeMillis()
		s"INSERT INTO ${options.table} (${columns}, sign, version) VALUES (${values.mkString(", ")}, 1, ${version})"
	}
	
	/**
	 * 生成修改操作的sql语句
	 */
	def createUpdateSQL(schema: StructType, row: InternalRow): String = {
		/*
			ALTER TABLE table UPDATE fieldName = '', ... WHERE id = 3, ...
		 */
		// 获取所有Field字段
		val fields: Array[StructField] = schema.fields
		
		// 获取主键列名称
		val primaryKeyName: String = options.getPrimaryKey
		// 获取主键列值
		var primaryKeyValue: String = getFieldValue(primaryKeyName, schema, row)
		// 判断数据类型市是否是String、Date和DateTime，如果是值使用 字符串表示
		val primaryKeyDataType: DataType = fields(schema.fieldIndex(primaryKeyName)).dataType
		primaryKeyValue = convertValue(primaryKeyValue, primaryKeyDataType)
		
		// 获取操作类型字段列名称
		val operateFieldName: String = options.getOperateField
		val updateColumns: Array[String] = schema.names
			.filter(name => ! primaryKeyName.equals(name))
			.filter(name => ! operateFieldName.equals(name))
			.map{fieldName =>
				// 获取类型
				var fieldValue: String = getFieldValue(fieldName, schema, row)
				// 获取列值
				val fieldDataType: DataType = fields(schema.fieldIndex(fieldName)).dataType
				// 判断数据类型市是否是String、Date和DateTime，如果是值使用 字符串表示
				fieldValue = convertValue(fieldValue, fieldDataType)
				// 返回列值
				s"${fieldName} = ${fieldValue}"
			}
		
		// 基于ALTER语法构建更新Update语句
		s"ALTER TABLE ${options.table} UPDATE ${updateColumns.mkString(", ")} WHERE ${primaryKeyName} = ${primaryKeyValue}"
	}
	
	/**
	 * 生成删除操作的sql语句
	 */
	def createDeleteSQL(schema: StructType, row: InternalRow): String = {
		/*
			ALTER TABLE table DELETE WHERE primaryKey = primaryValue
		 */
		// 获取主键列名称
		val primaryKeyName: String = options.getPrimaryKey
		// 获取主键列值
		var primaryKeyValue: String = getFieldValue(primaryKeyName, schema, row)
		
		// 判断数据类型市是否是String、Date和DateTime，如果是值使用 字符串表示
		val primaryKeyDataType: DataType = schema.fields(schema.fieldIndex(primaryKeyName)).dataType
		primaryKeyValue = convertValue(primaryKeyValue, primaryKeyDataType)
		
		// 基于ALTER 语法构建删除语句
		s"ALTER TABLE ${options.table} DELETE WHERE ${primaryKeyName} = ${primaryKeyValue}"
	}
	
	/**
	 * 批量执行操作，执行INSERT、ALTER语句
	 */
	def executeUpdateBatch(sqls: Array[String]): Unit = {
		// 声明变量
		var conn: ClickHouseConnection = null
		var stmt: ClickHouseStatement = null
		try {
			// 创建实例对象
			conn = getConnection
			stmt = conn.createStatement()
			
			// TODO: 操作类型：INSERT
			val insertSQLs: Array[String] = sqls.filter(sql => sql.startsWith("INSERT"))
			val batchSQL = new StringBuilder()
			var counter: Int = 1
			insertSQLs.foreach{insertSQL =>
				if(1 == counter) {
					batchSQL.append(insertSQL)
				}else{
					// INSERT INTO test.tbl_order_agg (category, count, sign, version) VALUES ('电脑', 8, 1, 1610777043766)
					val offset = insertSQL.indexOf("VALUES")
					batchSQL.append(",").append(insertSQL.substring(offset + 6))
				}
				counter += 1
			}
			if(batchSQL.nonEmpty) {
				//println(batchSQL.toString())
				executeUpdate(batchSQL.toString())
			}
			
			// TODO: 操作类型：ALTER(UPDATE和DELETE)
			val alterSQLs: Array[String] = sqls.filter(sql => sql.startsWith("ALTER"))
			alterSQLs.foreach{alterSQL =>
				stmt.executeUpdate(alterSQL)
			}
		} catch {
			case e: Exception => e.printStackTrace()
		}finally {
			if (stmt != null || ! stmt.isClosed) stmt.close()
			if (conn != null || ! conn.isClosed) conn.close()
		}
	}
	
	/**
	 * 关闭连接释放资源
	 */
	def closeJdbc(conn: ClickHouseConnection,
	              stmt: ClickHouseStatement,
	              result: ResultSet): Unit = {
		if (result != null || !result.isClosed) result.close()
		if (stmt != null || !stmt.isClosed) stmt.close()
		if (conn != null || !conn.isClosed) conn.close()
	}
	
}

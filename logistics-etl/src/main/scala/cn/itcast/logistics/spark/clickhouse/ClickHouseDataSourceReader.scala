package cn.itcast.logistics.spark.clickhouse

import java.sql.ResultSet
import java.util

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, InputPartitionReader}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import ru.yandex.clickhouse.{ClickHouseConnection, ClickHouseStatement}

/**
 * SparkSQL批量加载数据源ClickHouse表中的数据，封装DataFrame
 *      DataFrame = RDD[Row] + Schema
 */
class ClickHouseDataSourceReader(options: ClickHouseOptions) extends DataSourceReader{
	
	/**
	 * 数据集DataFrame约束Schema
	 */
	override def readSchema(): StructType = {
		// step1、获取ClickHouseHelper对象
		val helper: ClickHouseHelper = new ClickHouseHelper(options)
		// step2、获取Schema信息 -> 首先，获取ClickHouse表Schema信息；然后，转换构建DataFrame中Schema信息
		val schema: StructType = helper.getSparkSQLSchema
		// step3、返回构建Schema信息
		schema
	}
	
	/**
	 * 读取数据源ClickHouse表的数据，按照分区进行读取，每条数据封装在Row对象中
	 */
	override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {
		// TODO: 读取ClickHouse表数据时，就封装在一个分区
		util.Arrays.asList(new ClickHouseInputPartition(options, readSchema()))
	}
}

/**
 *每个分区数据对象，读取ClickHouse表数据时，将数据划分为不同Partition，称为InputPartition
 *      TODO: 主构造方法中参数schema，从ClickHouse表加载数据以后，依据字段名称和类型获取对应类型的值
 */
class ClickHouseInputPartition(options: ClickHouseOptions,
                               schema: StructType) extends InputPartition[InternalRow] {
	/**
	 * 创建每个分区数据读取数据读取器Reader
	 */
	override def createPartitionReader(): InputPartitionReader[InternalRow] = {
		new ClickHouseInputPartitionReader(options, schema)
	}
	
}

/**
 * 读取数据时，将数据划分为不同分区，每个分区数据加载时，通过Reader读取器加载数据
 */
class ClickHouseInputPartitionReader(options: ClickHouseOptions,
                                     schema: StructType) extends InputPartitionReader[InternalRow] {
	// 创建ClickHouseHelper对象，传递参数
	private val clickHouseHelper: ClickHouseHelper = new ClickHouseHelper(options)
	
	// 定义变量
	var conn: ClickHouseConnection = _
	var stmt: ClickHouseStatement = _
	var result: ResultSet = _
	
	/**
	 * 判断是否还有数据要被读取，如果返回true，则调用get方法，获取数据；如果返回false，表示没有数据，调用close，关闭连接
	 */
	override def next(): Boolean = {
		// 查询 ClickHouse 表中的数据，根据查询结果判断是否存在数据
		if((conn == null || conn.isClosed)
			&& (stmt == null || stmt.isClosed)
			&& (result == null || result.isClosed)){
			// 实例化connection连接对象
			conn = clickHouseHelper.getConnection
			stmt = conn.createStatement()
			
			val selectSQL: String = clickHouseHelper.getSelectStatementSQL(schema)
			println("Query SQL: " + selectSQL)
			result = stmt.executeQuery(selectSQL)
			// println("=======初始化ClickHouse数据库成功===========")
		}
		
		// 如果查询结果集对象不是空并且没有关闭的话在，则指针下移
		if(result != null && !result.isClosed){
			// 如果next是true，表示有数据，否则没有数据
			result.next()
		}else{
			// 返回false表示没有数据
			false
		}
	}
	
	/**
	 * 当next返回true时，表示还有数据，调用此方法读取
	 */
	override def get(): InternalRow = {
		// TODO: next()返回true，则该方法被调用，如果返回false，该方法不被调用
		// println("======调用get函数，获取当前数据============")
		val fields: Array[StructField] = schema.fields
		//一条数据所有字段的集合
		val record: Array[Any] = new Array[Any](fields.length)
		
		// 循环取出来所有的列
		for (i <- record.indices) {
			// 每个字段信息
			val field: StructField = fields(i)
			// 列名称
			val fieldName: String = field.name
			// 列数据类型
			val fieldDataType: DataType = field.dataType
			// 根据字段类型，获取对应列的值
			fieldDataType match {
				case DataTypes.BooleanType => record(i) = result.getBoolean(fieldName)
				case DataTypes.DateType => record(i) = DateTimeUtils.fromJavaDate(result.getDate(fieldName))
				case DataTypes.DoubleType => record(i) = result.getDouble(fieldName)
				case DataTypes.FloatType => record(i) = result.getFloat(fieldName)
				case DataTypes.IntegerType => record(i) = result.getInt(fieldName)
				case DataTypes.ShortType => record(i) = result.getShort(fieldName)
				case DataTypes.LongType => record(i) = result.getLong(fieldName)
				case DataTypes.StringType => record(i) = UTF8String.fromString(result.getString(fieldName))
				case DataTypes.TimestampType => record(i) = DateTimeUtils.fromJavaTimestamp(result.getTimestamp(fieldName))
				case DataTypes.ByteType => record(i) = result.getByte(fieldName)
				case DataTypes.NullType => record(i) = StringUtils.EMPTY
				case _ => record(i) = StringUtils.EMPTY
			}
		}
		
		// 创建InternalRow对象
		new GenericInternalRow(record)
	}
	
	/**
	 * 当next返回false时，表示没有数据，关闭所有连接
	 */
	override def close(): Unit = {
		clickHouseHelper.closeJdbc(conn, stmt, result)
	}
	
}
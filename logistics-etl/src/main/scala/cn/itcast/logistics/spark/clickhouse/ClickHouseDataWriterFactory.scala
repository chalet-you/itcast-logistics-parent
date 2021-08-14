package cn.itcast.logistics.spark.clickhouse

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ArrayBuffer

/**
 * 将DataFrame数据批量保存时，构建数据写入器工厂对象，主要为每个分区创建DataWriter对象
 */
class ClickHouseDataWriterFactory(options: ClickHouseOptions, schema: StructType) extends DataWriterFactory[InternalRow]{
	
	/**
	 * 为每个分区Partition数据保存时，创建DataWriter数据写入器对象
	 */
	override def createDataWriter(partitionId: Int,
	                              taskId: Long,
	                              epochId: Long): DataWriter[InternalRow] = {
		new ClickHouseDataWriter(options, schema)
	}
}

/**
 * 数据写入器类，每个分区数据保存时，都要创建对象，保存分区数据
 */
class ClickHouseDataWriter(options: ClickHouseOptions, schema: StructType) extends DataWriter[InternalRow]{
	
	// 创建ClickHouseHelper对象
	val clickHouseHelper: ClickHouseHelper = new ClickHouseHelper(options)
	
	// 初始化操作，当构建DataWriter对象时，判断ClickHouse表是否存在，如果不存在，自动创建
	private val init = {
		// 如果允许自动创建表，依据DataFrame信息创建表
		if(options.autoCreateTable){
			// 生成建表的DDL语句
			val ddl: String = clickHouseHelper.createTableDdl(schema)
			println(s"${ddl}")
			// 表的创建
			clickHouseHelper.executeUpdate(ddl)
		}
	}
	
	// 将需要操作SQL语句存储到集合列表中
	private var sqlArray: ArrayBuffer[String] = ArrayBuffer[String]()
	
	/**
	 * 写入数据（被多次调用，每条数据都会调用一次该方法）
	 */
	override def write(record: InternalRow): Unit = {
		// 根据当前数据的操作类型，生成不同的sql语句，如果OpType是insert，那么生成插入的sql语句
		val sqlStr: String = clickHouseHelper.createStatementSQL(schema, record)
		//println(s"${sqlStr}")
		
		// 如果生成的更新操作的sql语句为空，则不需要追加到更新操作的sql集合中
		if(StringUtils.isEmpty(sqlStr)){
			val message = "===========拼接的插入、更新、删除操作的sql语句失败================"
			throw new RuntimeException(message)
		}
		
		// 将当前操作的sql语句写入到sql集合中
		sqlArray += sqlStr
	}
	
	/**
	 *  数据提交成功以后，返回什么信息
	 *  TODO: 提交方法（提交数据），可以用于批量提交（批量保存数据到外部存储系统）
	 */
	override def commit(): WriterCommitMessage = {
		// 真正的数据更新操作，将数据更新的到 ClickHouse数据库中
		if (sqlArray.nonEmpty) {
			//将数据操作的sql语句执行，更新到clickhouse表中
			clickHouseHelper.executeUpdateBatch(sqlArray.toArray)
		}
		
		// 清空列表
		sqlArray.clear()
		
		// 提交执行后，返回给Client消息
		new WriterCommitMessage() {
			override def toString: String = s"批量更新SQL语句：${sqlArray.mkString("\n")}"
		}
	}
	
	/**
	 * 保存数据异常，事务回滚时，一些操作信息
	 * TODO: 此处不考虑异常情况
	 */
	override def abort(): Unit = {
		// TODO: 咋不考虑代码实现
	}
}
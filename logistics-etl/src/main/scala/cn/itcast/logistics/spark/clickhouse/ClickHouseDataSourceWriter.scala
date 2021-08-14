package cn.itcast.logistics.spark.clickhouse

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

/**
 * 批量保存数据至ClickHouse表中时，Writer对象
 */
class ClickHouseDataSourceWriter(options: ClickHouseOptions, schema: StructType) extends DataSourceWriter{
	
	// 返回数据写入器工厂DataWriterFactory对象，DataFrame中有多个分区，每个分区数据写入时，需要DataWriter写入器，由工厂创建
	override def createWriterFactory(): DataWriterFactory[InternalRow] = {
		new ClickHouseDataWriterFactory(options, schema)
	}
	
	/**
	 * 当写入数据成功时，信息动作
	 */
	override def commit(messages: Array[WriterCommitMessage]): Unit = {
		// TODO: 此处简化，无任何操作
	}
	
	/**
	 * 当数据写入失败时，信息动作
	 */
	override def abort(messages: Array[WriterCommitMessage]): Unit = {
		// TODO: 此处简化，无任何代码
	}
}

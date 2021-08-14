package cn.itcast.logistics.spark.sql

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriterFactory, WriterCommitMessage}

/**
 * 构建数据写入器Writer对象，用于将DataFrame中各个分区数据写入到数据源
 */
class CustomerDataSourceWriterV2 extends DataSourceWriter{
	// 获取写入器工厂，创建Writer对象，每个分区数据，使用一个写入器写入数据
	override def createWriterFactory(): DataWriterFactory[InternalRow] = ???
	
	// 写入程序，返回结果
	override def commit(messages: Array[WriterCommitMessage]): Unit = ???
	
	// 写入失败，相关处理
	override def abort(messages: Array[WriterCommitMessage]): Unit = ???
}

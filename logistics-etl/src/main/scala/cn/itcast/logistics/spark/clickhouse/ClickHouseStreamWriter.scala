package cn.itcast.logistics.spark.clickhouse

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.types.StructType

/**
 * 结构化流每批次数据保存，继承接口StreamWriter，本质上还是批量保存
 */
class ClickHouseStreamWriter(options: ClickHouseOptions, schema: StructType) extends StreamWriter{
	// 每个微批次数据DataFrame保存时，首先构建数据写入器工厂，为每个分区创建写入器对象
	override def createWriterFactory(): DataWriterFactory[InternalRow] = {
		new ClickHouseDataWriterFactory(options, schema)
	}
	
	override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
		// 提交成功，暂不考虑，代码实现
	}
	
	override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
		// 数据保存失败，暂不考虑，代码实现
	}
	
}

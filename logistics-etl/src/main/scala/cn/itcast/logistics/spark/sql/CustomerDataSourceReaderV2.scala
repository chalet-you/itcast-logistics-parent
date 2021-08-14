package cn.itcast.logistics.spark.sql

import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition}
import org.apache.spark.sql.types.StructType

/**
 * 构建读取器对象，用于从外部数据源加载数据
 */
class CustomerDataSourceReaderV2 extends DataSourceReader{
	// 读取数据封装的Schema信息
	override def readSchema(): StructType = ???
	
	// 读取数据时，将数据划分为不同分区，每个分区中数据就是每条数据，封装在Row中
	override def planInputPartitions(): util.List[InputPartition[InternalRow]] = ???
}

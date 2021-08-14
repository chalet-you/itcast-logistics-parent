package cn.itcast.logistics.spark.sql

import java.util.Optional

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, WriteSupport}
import org.apache.spark.sql.types.StructType

/**
 * 实现SparkSQL外部数据源接口，完成批量加载数据和批量保存，封装到DataFrame中
 */
class CustomerDataSourceV2 extends DataSourceV2 with ReadSupport with WriteSupport{
	/**
	 * 构建数据源数据读取器Reader，用于加载数据
	 *
	 * @param options
	 * @return
	 */
	override def createReader(options: DataSourceOptions): DataSourceReader = {
		new CustomerDataSourceReaderV2()
	}
	
	/**
	 * 构建数据源数据写入器Writer对象，用于保存数据
	 * @param writeUUID
	 * @param schema
	 * @param mode
	 * @param options
	 * @return
	 */
	override def createWriter(writeUUID: String,
	                          schema: StructType,
	                          mode: SaveMode,
	                          options: DataSourceOptions): Optional[DataSourceWriter] = {
		mode match {
			case SaveMode.Append =>
				val writer = new CustomerDataSourceWriterV2()
				Optional.of(writer)
			case _ =>
				println("Current Only Support Mode: Append Mode。。。。。。。。。。。。。。")
				Optional.empty()
		}
	}
}



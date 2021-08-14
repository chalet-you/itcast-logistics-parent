package cn.itcast.logistics.spark.clickhouse

import java.util.Optional

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, StreamWriteSupport, WriteSupport}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

/**
 * 依据SparkSQL中DataSource V2接口，自定义实现ClickHouse外部数据源，批量读写数据和流式写入数据
 */
class ClickHouseDataSourceV2 extends DataSourceV2 with DataSourceRegister
							 with ReadSupport with WriteSupport
                             with StreamWriteSupport{
	/**
	 * 指定数据源时，使用简短名
	 */
	override def shortName(): String = "clickhouse"
	
	/**
	 * 从外部数据源读取数据Reader(ReadSupport 方法）
	 *
	 * @param options 加载数据时传递option参数
	 */
	override def createReader(options: DataSourceOptions): DataSourceReader = {
		// step1、解析传递参数
		val clickHouseOptions: ClickHouseOptions = new ClickHouseOptions(options.asMap())
		
		// step2、构建自定义读取Reader对象，返回即可
		val reader: ClickHouseDataSourceReader = new ClickHouseDataSourceReader(clickHouseOptions)
		
		// step3、返回读取器对象
		reader
	}
	
	/**
	 * 将数据保存外部数据源Writer（WriteSupport方法）
	 *
	 * @param writeUUID 表示JobID，针对SparkSQL中每个Job保存来说，就是JobID
	 * @param schema    保存数据Schema约束信息
	 * @param mode      保存模式
	 * @param options   保存数据时传递option参数
	 */
	override def createWriter(writeUUID: String,
	                          schema: StructType,
	                          mode: SaveMode,
	                          options: DataSourceOptions): Optional[DataSourceWriter] = {
		// 依据保存模式，决定是否返回数据源写入器对象，如果不支持模式，直接结束程序
		mode match {
			case SaveMode.Append =>
				// step1、解析传递参数
				val clickHouseOptions: ClickHouseOptions = new ClickHouseOptions(options.asMap())
				// step2、创建数据源写入器对象
				val writer: ClickHouseDataSourceWriter = new ClickHouseDataSourceWriter(clickHouseOptions, schema)
				// step3、 返回数据
				Optional.of(writer)
			case _ =>
				println("Current Only Support: Append Mode ...................")
				Optional.empty()
		}
	}
	
	/**
	 * 将流式数据中每批次结果保存外部数据源StreamWriter（StreamWriteSupport方法）
	 *
	 * @param queryId 流式应用中查询ID（StreamingQuery ID）
	 * @param schema  保存数据Schema约束
	 * @param mode    输出模式
	 * @param options 保存数据时传递option参数
	 */
	override def createStreamWriter(queryId: String,
	                                schema: StructType,
	                                mode: OutputMode,
	                                options: DataSourceOptions): StreamWriter = {
		// step1、解析传递参数
		val clickHouseOptions: ClickHouseOptions = new ClickHouseOptions(options.asMap())
		// step2、创建数据源写入器对象
		val writer: ClickHouseStreamWriter = new ClickHouseStreamWriter(clickHouseOptions, schema)
		// step3、返回流式写入器对象
		writer
	}
	

}

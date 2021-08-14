package cn.itcast.logistics.etl.realtime

import cn.itcast.logistics.common.Configuration
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 所有ETL流式处理的基类，实时增量ETL至：Kudu、Elasticsearch和ClickHouse都要实现此基类，定义三个方法
	 * - 1. 加载数据：load
	 * - 2. 处理数据：process
	 * - 3. 保存数据：save
 */
trait BasicStreamApp {
	
	/**
	 * 读取数据的方法
	 *
	 * @param spark SparkSession
	 * @param topic 指定消费的主题
	 * @param selectExpr 默认值：CAST(value AS STRING)
	 */
	def load(spark: SparkSession, topic: String, selectExpr: String = "CAST(value AS STRING)"): DataFrame = {
		spark.readStream
			.format(Configuration.SPARK_KAFKA_FORMAT)
			.option("kafka.bootstrap.servers", Configuration.KAFKA_ADDRESS)
			.option("subscribe", topic)
			.option("maxOffsetsPerTrigger", "100000")
			.load()
			.selectExpr(selectExpr)
	}
	
	/**
	 * 数据的处理
	 *
	 * @param streamDF 流式数据集StreamingDataFrame
	 * @param category 业务数据类型，比如物流系统业务数据，CRM系统业务数据等
	 * @return 流式数据集StreamingDataFrame
	 */
	def process(streamDF: DataFrame, category: String): DataFrame
	
	/**
	 * 数据的保存
	 *
	 * @param streamDF 保存数据集DataFrame
	 * @param tableName 保存表的名称
	 * @param isAutoCreateTable 是否自动创建表，默认创建表
	 */
	def save(streamDF: DataFrame, tableName: String, isAutoCreateTable: Boolean = true): Unit
	
}

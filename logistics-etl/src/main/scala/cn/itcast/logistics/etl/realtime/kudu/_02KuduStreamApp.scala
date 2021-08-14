package cn.itcast.logistics.etl.realtime.kudu

import cn.itcast.logistics.common.SparkUtils
import cn.itcast.logistics.etl.realtime.BasicStreamApp
import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SparkSession}

object _02KuduStreamApp extends BasicStreamApp {
	
	/**
	 * 数据的处理
	 *
	 * @param streamDF 流式数据集StreamingDataFrame
	 * @param category 业务数据类型，比如物流系统业务数据，CRM系统业务数据等
	 * @return 流式数据集StreamingDataFrame
	 */
	override def process(streamDF: DataFrame, category: String): DataFrame = {
		// TODO: 依据业务系统类型，对业务数据进行相关转换ETL操作
		category match {
			// 物流系统业务数据处理转换
			case "logistics" =>
				streamDF
			// CRM 系统业务数据处理转换
			case "crm" =>
				streamDF
			// 其他业务系统处理转换
			case _ => streamDF
		}
	}
	
	/**
	 * 数据的保存
	 *
	 * @param streamDF          保存数据集DataFrame
	 * @param tableName         保存表的名称
	 * @param isAutoCreateTable 是否自动创建表，默认创建表
	 */
	override def save(streamDF: DataFrame, tableName: String, isAutoCreateTable: Boolean): Unit = {
		streamDF.writeStream
			.queryName(tableName)
			.outputMode(OutputMode.Append())
			.format("console")
			.option("numRows", "20")
			.option("truncate", "false")
			.start()
	}
	
	/*
		流式应用程序入门MAIN方法，代码逻辑步骤：
			step1. 创建SparkSession实例对象，传递SparkConf
			step2. 从Kafka数据源实时消费数据
			step3. 对获取Json数据进行ETL转换
			step4. 保存转换后数据到外部存储
			step5. 应用启动以后，等待终止结束
	 */
	def main(args: Array[String]): Unit = {
		// step1. 创建SparkSession实例对象，传递SparkConf
		val spark: SparkSession = {
			// SparkUtils.createSparkSession(SparkUtils.autoSettingEnv(SparkUtils.sparkConf()), this.getClass)
			// a. 获取SparkConf对象
			val sparkConf: SparkConf = SparkUtils.sparkConf()
			// b. 自动依据运行环境，设置运行模式
			val conf = SparkUtils.autoSettingEnv(sparkConf)
			// c. 获取SparkSession对象
			SparkUtils.createSparkSession(conf, this.getClass)
		}
		
		// step2. 从Kafka数据源实时消费数据
		// step3. 对获取Json数据进行ETL转换
		// step4. 保存转换后数据到外部存储
		val logisticsStreamDF: DataFrame = load(spark, "logistics")
		val logisticsEtlStreamDF = process(logisticsStreamDF, "logistics")
		save(logisticsEtlStreamDF, "console-logistics")
		
		val crmStreamDF: DataFrame = load(spark, "crm")
		val crmEtlStreamDF: DataFrame = process(crmStreamDF, "crm")
		save(crmEtlStreamDF, "console-crm")

		// step5. 应用启动以后，等待终止结束
		spark.streams.active.foreach(query => println(s"Query: ${query.name} is Running ............"))
		spark.streams.awaitAnyTermination()
	}
}

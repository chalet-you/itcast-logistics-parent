package cn.itcast.logistics.etl.realtime.kudu

import cn.itcast.logistics.etl.realtime.BasicStreamApp
import org.apache.spark.sql.DataFrame

object _01KuduStreamApp extends BasicStreamApp {
	/**
	 * 数据的处理
	 *
	 * @param streamDF 流式数据集StreamingDataFrame
	 * @param category 业务数据类型，比如物流系统业务数据，CRM系统业务数据等
	 * @return 流式数据集StreamingDataFrame
	 */
	override def process(streamDF: DataFrame, category: String): DataFrame = ???
	
	/**
	 * 数据的保存
	 *
	 * @param streamDF          保存数据集DataFrame
	 * @param tableName         保存表的名称
	 * @param isAutoCreateTable 是否自动创建表，默认创建表
	 */
	override def save(streamDF: DataFrame, tableName: String, isAutoCreateTable: Boolean): Unit = ???
	
	/*
		流式应用程序入门MAIN方法，代码逻辑步骤：
			step1. 创建SparkSession实例对象，传递SparkConf
			step2. 从Kafka数据源实时消费数据
			step3. 对获取Json数据进行ETL转换
			step4. 保存转换后数据到外部存储
			step5. 应用启动以后，等待终止结束
	 */
	def main(args: Array[String]): Unit = {
	
	}
}

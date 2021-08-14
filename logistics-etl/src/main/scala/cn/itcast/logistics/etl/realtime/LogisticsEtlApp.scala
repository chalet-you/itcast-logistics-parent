package cn.itcast.logistics.etl.realtime

import cn.itcast.logistics.common.Configuration
import org.apache.commons.lang3.SystemUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 编写流式计算程序，测试实时从Kafka Topic消费业务系统数据，具体实现步骤如下：
 *  - 第一部分、编写程序【模板】
 *  - 第二部分、代码编写，消费数据，打印控制台
 *  - 第三部分、测试，启动MySQL数据库和Canal及Oracle数据库和OGG。
 */
object LogisticsEtlApp {
	
	/*
		1. 初始化设置Spark Application配置
		2. 判断Spark Application运行模式进行设置
		3. 构建SparkSession实例对象
		
		4. 初始化消费物流Topic数据参数
		5. 消费物流Topic数据，打印控制台
		6. 初始化消费CRM Topic数据参数
		7. 消费CRM Topic数据，打印控制台
		
		8. 启动流式应用，等待终止
	 */
	def main(args: Array[String]): Unit = {
		// step1、构建SparkSession实例对象，用于加载数据源的数据 -> [1、2、3]
		val spark: SparkSession = {
			// a. 创建SparkConf对象，设置应用属性
			val sparkConf: SparkConf = new SparkConf()
				// TODO: 在实际项目开发中，对每个应用需求调整参数值 -> 公共参数【sparkConf】和特有采参数【--conf】
				.set("spark.sql.session.timeZone", "Asia/Shanghai")
				.set("spark.sql.files.maxPartitionBytes", "134217728")
				.set("spark.sql.files.openCostInBytes", "134217728")
				.set("spark.sql.shuffle.partitions", "3")
				.set("spark.sql.autoBroadcastJoinThreshold", "67108864")
			// b. 依据应用程序，运行操作系统，设置运行模式：本地，还是集群
			if (SystemUtils.IS_OS_WINDOWS || SystemUtils.IS_OS_MAC) {
				//本地环境LOCAL_HADOOP_HOME
				System.setProperty("hadoop.home.dir", Configuration.LOCAL_HADOOP_HOME)
				//设置运行环境和checkpoint路径
				sparkConf
					.set("spark.master", "local[3]")
					.set("spark.sql.streaming.checkpointLocation", Configuration.SPARK_APP_WIN_CHECKPOINT_DIR)
			} else {
				//生产环境
				sparkConf
					.set("spark.master", "yarn")
    				.set("spark.submit.deployMode", "cluster")
					.set("spark.sql.streaming.checkpointLocation", Configuration.SPARK_APP_DFS_CHECKPOINT_DIR)
			}
			// c. 传递应用名称SparkConf对象，构建实例
			SparkSession.builder()
    			.appName(this.getClass.getSimpleName.stripSuffix("$"))
    			.config(sparkConf)
    			.getOrCreate()
		}
		import spark.implicits._
		
		// step2、从Kafka加载业务数据和打印控制台 -> [4、5、6、7]
		val logisticsDF: DataFrame = spark.readStream
			.format("kafka")
			.option("kafka.bootstrap.servers", Configuration.KAFKA_ADDRESS)
			.option("subscribe", Configuration.KAFKA_LOGISTICS_TOPIC)
			.load()
			.selectExpr("CAST(value AS STRING)")
		val logisticsQuery = logisticsDF.writeStream
			.queryName("query-logistics-test")
			.outputMode(OutputMode.Append())
			.format("console")
			.option("numRows", "10")
			.option("truncate", "false")
			.start()
		
		val crmDF: DataFrame = spark.readStream
			.format("kafka")
			.option("kafka.bootstrap.servers", Configuration.KAFKA_ADDRESS)
			.option("subscribe", Configuration.KAFKA_CRM_TOPIC)
			.load()
			.selectExpr("CAST(value AS STRING)")
		val crmQuery = crmDF.writeStream
			.queryName("query-crm-test")
			.outputMode(OutputMode.Append())
			.format("console")
			.option("numRows", "10")
			.option("truncate", "false")
			.start()
		
		// step3、启动流式应用，等待终止 -> [8]
		spark.streams.active.foreach(query => println(s"Query: ${query.name} is Running..............."))
		spark.streams.awaitAnyTermination()
	}
	
}

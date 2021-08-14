package cn.itcast.logistics.common

import org.apache.commons.lang3.SystemUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Spark 操作的工具类，主要获取SparkSession实例对象
 */
object SparkUtils {
	
	// TODO: 在Scala语言中，函数也是一等公民，可以赋值给变量，比如匿名函数，赋值为变量
	// 定义变量，类型匿名函数类型，返回为SparkConf对象
	lazy val sparkConf = () => {
		new SparkConf()
			.set("spark.sql.session.timeZone", "Asia/Shanghai")
			.set("spark.sql.files.maxPartitionBytes", "134217728")
			.set("spark.sql.files.openCostInBytes", "134217728")
			.set("spark.sql.shuffle.partitions", "3")
			.set("spark.sql.autoBroadcastJoinThreshold", "67108864")
	}
	
	// 依据应用运行操作系统，设置运行模式：local本地、yarn集群
	lazy val autoSettingEnv = (sparkConf: SparkConf) => {
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
				.set("spark.sql.streaming.checkpointLocation", Configuration.SPARK_APP_DFS_CHECKPOINT_DIR)
		}
		// 返回设置以后SparkConf对象
		sparkConf
	}
	
	/**
	 * 传递应用Class对象和SparkConf配置实例，构建会话Session对象
	 */
	def createSparkSession(sparkConf: SparkConf, clazz: Class[_]): SparkSession = {
		SparkSession.builder()
			.appName(clazz.getSimpleName.stripSuffix("$"))
			.config(sparkConf)
			.getOrCreate()
	}
	
	
	// 测试
	def main(args: Array[String]): Unit = {
		val spark: SparkSession = createSparkSession(
			autoSettingEnv(sparkConf()), this.getClass
		)
		println(spark)
		
		Thread.sleep(10000000)
		spark.stop()
	}
	
}

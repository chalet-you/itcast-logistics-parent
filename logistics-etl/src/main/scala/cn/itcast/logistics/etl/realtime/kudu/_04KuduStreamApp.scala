package cn.itcast.logistics.etl.realtime.kudu

import cn.itcast.logistics.common.beans.logistics.AreasBean
import cn.itcast.logistics.common.beans.parser.{CanalMessageBean, OggMessageBean}
import cn.itcast.logistics.common.{SparkUtils, TableMapping}
import cn.itcast.logistics.etl.parser.DataParser
import cn.itcast.logistics.etl.realtime.BasicStreamApp
import com.alibaba.fastjson.JSON
import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession}

object _04KuduStreamApp extends BasicStreamApp {
	
	/**
	 * 数据的处理
	 *
	 * @param streamDF 流式数据集StreamingDataFrame
	 * @param category 业务数据类型，比如物流系统业务数据，CRM系统业务数据等
	 * @return 流式数据集StreamingDataFrame
	 */
	override def process(streamDF: DataFrame, category: String): DataFrame = {
		// 此处streamDF表示直接从Kafka获取流式数据集，默认只有一个字段：value，类型String
		
		import streamDF.sparkSession.implicits._
		
		// TODO: 依据业务系统类型，对业务数据进行相关转换ETL操作
		category match {
			// 物流系统业务数据处理转换
			case "logistics" =>
				// step1. JSON -> MessageBean
				val beanDS: Dataset[OggMessageBean] = streamDF
					.as[String] // 转换DataFrame为Dataset
					// 过滤掉脏数据
					.filter(message => null != message && message.trim.length > 0)
					// 解析每条数据
					.map { message =>
						JSON.parseObject(message.trim, classOf[OggMessageBean])
					}(Encoders.bean(classOf[OggMessageBean]))
				
				// step2. MessageBean -> POJO
				// TODO: 此处以表【tbl_areas】为例，将其数据封装到POJO对象中
				import cn.itcast.logistics.common.BeanImplicits._
				val pojoDS: Dataset[AreasBean] = beanDS
					// 依据表的名称，过滤获取相关数据 ->  "tbl_areas" == bean.getTable
					.filter(bean => TableMapping.AREAS.equals(bean.getTable))
					// 提取bean字段值（opType和getvalue）封装POJO对象
					.map(bean => DataParser.toAreaBean(bean))
					// 再次过滤，如果数据转换pojo对象失败，返回null值
					.filter(pojo => null != pojo)
				
				// 返回转换后数据
				pojoDS.toDF()
				
			// CRM 系统业务数据处理转换
			case "crm" =>
				// 定义隐式参数，传递编码器值
				implicit val encoder: Encoder[CanalMessageBean] = Encoders.bean(classOf[CanalMessageBean])
				val beanDS: Dataset[CanalMessageBean] = streamDF
					// 过滤数据
					.filter(row => !row.isNullAt(0))
					// 解析数据
					.map { row =>
						val message: String = row.getString(0)
						JSON.parseObject(message.trim, classOf[CanalMessageBean])
					}
				
				// 返回转换后的数据
				beanDS.toDF()
				
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
		
//		val crmStreamDF: DataFrame = load(spark, "crm")
//		val crmEtlStreamDF: DataFrame = process(crmStreamDF, "crm")
//		save(crmEtlStreamDF, "console-crm")

		// step5. 应用启动以后，等待终止结束
		spark.streams.active.foreach(query => println(s"Query: ${query.name} is Running ............"))
		spark.streams.awaitAnyTermination()
	}
}

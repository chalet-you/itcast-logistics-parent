package cn.itcast.logistics.etl.realtime

import cn.itcast.logistics.common.{Configuration, SparkUtils, TableMapping}
import cn.itcast.logistics.common.beans.parser.OggMessageBean
import cn.itcast.logistics.common.BeanImplicits._
import cn.itcast.logistics.etl.parser.DataParser
import cn.itcast.logistics.etl.realtime.KuduStreamApp.save
import com.alibaba.fastjson.JSON
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode

/**
 * 编写结构化流程序，实时从Kafka消费业务数据，进行ETL转换后，存储到Es索引中，便于查询检索
 */
object ElasticsearchStreamApp extends BasicStreamApp {
	/**
	 * 数据的处理，解析JSON数据为MessageBean对象
	 *
	 * @param streamDF 流式数据集StreamingDataFrame
	 * @param category 业务数据类型，比如物流系统业务数据，CRM系统业务数据等
	 * @return 流式数据集StreamingDataFrame
	 */
	override def process(streamDF: DataFrame, category: String): DataFrame = {
		category match {
			case "logistics" =>
				// TODO: 由于仅仅同步核心业务数据到ES索引，所以先过滤，后转换
				streamDF
					// 仅仅获取快递单和运单数据
    				.where(
					    get_json_object(col("value"), "$.table").isin("ITCAST.tbl_express_bill", "ITCAST.tbl_waybill")
				    )
					// 再次简易数据
    				.filter(col("value").isNotNull && length(trim(col("value"))).gt(0))
					// 解析数据
    				.map{row =>
					    val message: String = row.getString(0)
					    JSON.parseObject(message.trim, classOf[OggMessageBean])
				    }
					// 转换为DataFrame
    				.toDF()
			case _ =>
				streamDF
		}
	}
	
	/**
	 * 数据的保存，存储至Es索引中
	 *
	 * @param streamDF          保存数据集DataFrame
	 * @param tableName         保存表的名称
	 * @param isAutoCreateTable 是否自动创建表，默认创建表
	 */
	override def save(streamDF: DataFrame, tableName: String, isAutoCreateTable: Boolean = true): Unit = {
		streamDF
			// 快递下单以后，要么修改和取消；运单来说，就是插入数据
			.drop(col("opType"))
			.writeStream
			.outputMode(OutputMode.Append())
			.queryName(s"query-${Configuration.SPARK_ES_FORMAT}-${tableName}")
			.format(Configuration.SPARK_ES_FORMAT)
			.option("es.nodes", Configuration.ELASTICSEARCH_HOST)
			.option("es.port", Configuration.ELASTICSEARCH_HTTP_PORT + "")
			.option(
				"es.index.auto.create", //
				if(isAutoCreateTable) "yes" else "no" //
			)
			.option("es.write.operation", "upsert")
			.option("es.mapping.id", "id")
			.start(tableName) // 参数含义：Elasticsearch中索引名称
	}
	
	/**
	 * 从ETL转换后Stream流式DataFrame中过滤获取express快递单和waybill运单数据，提取和解析存储到Es索引
	 */
	def etlEs(streamDF: DataFrame): Unit = {
		
		val beanStreamDS: Dataset[OggMessageBean] = streamDF.as[OggMessageBean]
		
		/*
			第一、从MessageBean提取字段，封装对应表POJO对象
			第二、保存数据到ES索引
		 */
		// TODO: 快递单业务数据 tbl_express_bill
		val expressBillStreamDF = beanStreamDS
			.filter(bean => bean.getTable == TableMapping.EXPRESS_BILL)
			.map(bean => DataParser.toExpressBill(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(expressBillStreamDF, TableMapping.EXPRESS_BILL)
		
		// TODO: 运单业务数据 tbl_waybill
		val waybillStreamDF = beanStreamDS
			.filter(bean => bean.getTable == TableMapping.WAY_BILL)
			.map(bean => DataParser.toWaybill(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(waybillStreamDF, TableMapping.WAY_BILL)
	}
	
	
	def main(args: Array[String]): Unit = {
		// step1、构建SparkSession实例对象
		val spark: SparkSession = SparkUtils.createSparkSession(
			SparkUtils.autoSettingEnv(SparkUtils.sparkConf()), this.getClass
		)
		import spark.implicits._
		
		// step2、从Kafka消费数据
		val streamDF: DataFrame = load(spark, "logistics")
		
		// step3、转换消费JSON数据
		val etlStreamDF: DataFrame = process(streamDF, "logistics")
		
		// step4、保存转换后数据
		etlEs(etlStreamDF)
		
		// step5、流式应用启动以后，等待终止
		spark.streams.active.foreach(query => println(s"Query: ${query.name} is Running ............"))
		spark.streams.awaitAnyTermination()
	}
}

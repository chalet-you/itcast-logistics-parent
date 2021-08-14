package cn.itcast.logistics.offline.dws

import cn.itcast.logistics.common.{Configuration, OfflineTableDefine}
import cn.itcast.logistics.offline.AbstractOfflineApp
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructType}
import org.apache.spark.storage.StorageLevel

/**
 * 车辆主题指标计算：
 *      网点车辆主题指标计算，从Kudu中DWD层加载宽表数据，按照业务指标进行计算，最终存储到Kudu中DWS层
 */
object TransportToolDotDWS extends AbstractOfflineApp{
	// 指标计算，加载宽表数据，依据业务指标进行统计
	override def process(dataframe: DataFrame): DataFrame = {
		// 获取Session会话实例，导入隐式
		val session: SparkSession = dataframe.sparkSession
		import session.implicits._
		
		// step1、获取日期day值，有多少天数据
		val days: Array[Row] = dataframe.select($"day").distinct().collect()
		
		// step2、遍历days，过滤获取每天数据，进行指标计算
		val aggRows: Array[Row] = days.map{ dayRow =>
			// 获取每日day值
			val dayValue: String = dayRow.getString(0)
			
			// 过滤每日数据
			val ttDetailDF: Dataset[Row] = dataframe.where($"day" === dayValue)
			ttDetailDF.persist(StorageLevel.MEMORY_AND_DISK)
			
			/* =========================== 指标计算 ============================== */
			// 指标一：网点发车次数及最大、最小和平均
			val ttDotTotalDF: DataFrame = ttDetailDF.groupBy($"dot_id").count()
			val ttDotTotalAggDF: DataFrame = ttDotTotalDF.agg(
				sum($"count").as("sumDotTotal"), // 使用sum函数，计算所有网点车次数之和
				max($"count").as("maxDotTotal"), //
				min($"count").as("minDotTotal"), //
				round(avg($"count"), 0).as("avgDotTotal") //
			)
			// 指标二：区域发车次数及最大、最小和平均
			val ttCityTotalDF: DataFrame = ttDetailDF.groupBy($"city_id").count()
			val ttCityTotalAggDF: DataFrame = ttCityTotalDF.agg(
				sum($"count").as("sumCityTotal"),  //
				max($"count").as("maxCityTotal"),  //
				min($"count").as("minCityTotal"), //
				round(avg($"count"), 0).as("avgCityTotal") //
			)
			// 指标三：公司发车次数及最大、最小和平均
			val ttCompanyTotalDF: DataFrame = ttDetailDF.groupBy($"company_id").count()
			val ttCompanyTotalAggDF: DataFrame = ttCompanyTotalDF.agg(
				sum($"count").as("sumCompanyTotal"),  //
				max($"count").as("maxCompanyTotal"),  //
				min($"count").as("minCompanyTotal"), //
				round(avg($"count"), 0).as("avgCompanyTotal") //
			)
			
			// 数据不在使用，释放资源
			ttDetailDF.unpersist()
			
			// TODO： 需要将计算所有指标结果提取出来，并且组合到Row对象中
			Row.fromSeq(
				dayRow.toSeq ++ //
					ttDotTotalAggDF.first().toSeq ++  //
					ttCityTotalAggDF.first().toSeq ++  //
					ttCompanyTotalAggDF.first().toSeq  //
			)
		}
		
		// step3、封装指标数据到DataFrame中
		// 第一步、将列表转换为RDD
		val rowsRDD: RDD[Row] = session.sparkContext.parallelize(aggRows.toList) // 将可变集合对象转换为不可变的
		// 第二步、自定义Schema信息
		val aggSchema: StructType = new StructType()
			.add("id", StringType, nullable = false) // 针对每天数据进行聚合得到一个结果，设置day为结果表中id
			.add("sumDotTotal", LongType, nullable = true)
			.add("maxDotTotal", LongType, nullable = true)
			.add("minDotTotal", LongType, nullable = true)
			.add("avgDotTotal", DoubleType, nullable = true)
			.add("sumCityTotal", LongType, nullable = true)
			.add("maxCityTotal", LongType, nullable = true)
			.add("minCityTotal", LongType, nullable = true)
			.add("avgCityTotal", DoubleType, nullable = true)
			.add("sumCompanyTotal", LongType, nullable = true)
			.add("maxCompanyTotal", LongType, nullable = true)
			.add("minCompanyTotal", LongType, nullable = true)
			.add("avgCompanyTotal", DoubleType, nullable = true)
		
		// 第三步、调用SparkSession中createDataFrame方法，组合RowsRDD和Schema为DataFrame
		val aggDF: DataFrame = session.createDataFrame(rowsRDD, aggSchema)
		
		// 返回指标结果
		aggDF
	}
	
	def main(args: Array[String]): Unit = {
		execute(
			this.getClass, //
			OfflineTableDefine.DOT_TRANSPORT_TOOL_DETAIL, //
			OfflineTableDefine.DOT_TRANSPORT_TOOL_SUMMARY, //
			isLoadFull = Configuration.IS_FIRST_RUNNABLE //
		)
	}
}

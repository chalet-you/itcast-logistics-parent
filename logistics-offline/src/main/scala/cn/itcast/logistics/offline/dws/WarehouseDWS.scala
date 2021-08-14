package cn.itcast.logistics.offline.dws

import cn.itcast.logistics.common.{Configuration, OfflineTableDefine}
import cn.itcast.logistics.offline.AbstractOfflineApp
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

/**
 * 仓库主题指标计算：
 *      从Kudu数据库中DWD层加载宽表数据，按照业务指标计算，可以编写DSL和SQL分析，最后返回DataFrame，保存至Kudu表中
 */
object WarehouseDWS extends AbstractOfflineApp{
	// 指标计算，按照业务指标对宽表数据进行计算
	override def process(dataframe: DataFrame): DataFrame = {
		// 导入隐式转换
		val session = dataframe.sparkSession
		import session.implicits._

		// step1. 获取日期day值
		val days: Array[Row] = dataframe.select($"day").distinct().collect()

		// step2. 遍历日期，过滤每天数据，进行指标计算
		val aggRow: Array[Row] = days.map{ dayRow =>
			// 获取每日day值
			val dayValue: String = dayRow.getString(0)

			// 过滤每日数据
			val warehouseDetailDF: Dataset[Row] = dataframe.filter($"day".equalTo(dayValue))
			warehouseDetailDF.persist(StorageLevel.MEMORY_AND_DISK)

			// 指标计算
			// 指标一、各仓库发车次数：最大、最小和平均
			val swTotalAggDF: DataFrame = warehouseDetailDF
				.groupBy($"sw_id").count()
				.agg(
					max($"count").as("swMaxTotal"), //
					min($"count").as("swMinTotal"), //
					round(avg($"count"), 0).as("swAvgTotal") //
				)

			// 指标二、各网点发车次数：最大、最小和平均
			val dotTotalAggDF: DataFrame = warehouseDetailDF
				.groupBy($"dot_id").count()
				.agg(
					max($"count").as("dotMaxTotal"), //
					min($"count").as("dotMinTotal"), //
					round(avg($"count"), 0).as("dotAvgTotal") //
				)

			// 指标三、各线路发车次数：最大、最小和平均
			val routeTotalAggDF: DataFrame = warehouseDetailDF
				.groupBy($"route_id").count()
				.agg(
					max($"count").as("routeMaxTotal"), //
					min($"count").as("routeMinTotal"), //
					round(avg($"count"), 0).as("routeAvgTotal") //
				)

			// 指标四、各类型客户发车次数：最大、最小和平均
			val ctypeTotalAggDF: DataFrame = warehouseDetailDF
				.groupBy($"ctype").count()
				.agg(
					max($"count").as("ctypeMaxTotal"), //
					min($"count").as("ctypeMinTotal"), //
					round(avg($"count"), 0).as("ctypeAvgTotal") //
				)

			// 指标五、各类型包裹发车次数：最大、最小和平均
			val packageTotalAggDF: DataFrame = warehouseDetailDF
				.groupBy($"package_id").count()
				.agg(
					max($"count").as("packageMaxTotal"), //
					min($"count").as("packageMinTotal"), //
					round(avg($"count"), 0).as("packageAvgTotal") //
				)

			// 指标六、各区域发车次数：最大、最小和平均
			val areaTotalAggDF: DataFrame = warehouseDetailDF
				.groupBy($"area_id").count()
				.agg(
					max($"count").as("areaMaxTotal"), //
					min($"count").as("areaMinTotal"), //
					round(avg($"count"), 0).as("areaAvgTotal") //
				)

			// 指标七、各公司发车次数：最大、最小和平均
			val companyTotalAggDF: DataFrame = warehouseDetailDF
				.groupBy($"company_id").count()
				.agg(
					max($"count").as("companyMaxTotal"), //
					min($"count").as("companyMinTotal"), //
					round(avg($"count"), 0).as("companyAvgTotal") //
				)

			// 数据不再使用，释放缓存
			warehouseDetailDF.unpersist()

			// 封装到Row对象中
			Row.fromSeq(
				dayRow.toSeq ++
					swTotalAggDF.first().toSeq ++
					dotTotalAggDF.first().toSeq ++
					routeTotalAggDF.first().toSeq ++
					ctypeTotalAggDF.first().toSeq ++
					packageTotalAggDF.first().toSeq ++
					areaTotalAggDF.first().toSeq ++
					companyTotalAggDF.first().toSeq
			)
		}

		// step3. 转换统计指标为DataFrame
		// a. RDD[Row]
		val rowRDD: RDD[Row] = session.sparkContext.parallelize(aggRow)
		// b. schema
		val schema = new StructType()
			.add("id", StringType, nullable = false)
			.add("maxSwTotal", LongType, nullable = true)
			.add("minSwTotal", LongType, nullable = true)
			.add("avgSwTotal", DoubleType, nullable = true)
			.add("maxDotTotal", LongType, nullable = true)
			.add("minDotTotal", LongType, nullable = true)
			.add("avgDotTotal", DoubleType, nullable = true)
			.add("maxRouteTotal", LongType, nullable = true)
			.add("minRouteTotal", LongType, nullable = true)
			.add("avgRouteTotal", DoubleType, nullable = true)
			.add("maxCtypeTotal", LongType, nullable = true)
			.add("minCtypeTotal", LongType, nullable = true)
			.add("avgCtypeTotal", DoubleType, nullable = true)
			.add("maxPackageTotal", LongType, nullable = true)
			.add("minPackageTotal", LongType, nullable = true)
			.add("avgPackageTotal", DoubleType, nullable = true)
			.add("maxAreaTotal", LongType, nullable = true)
			.add("minAreaTotal", LongType, nullable = true)
			.add("avgAreaTotal", DoubleType, nullable = true)
			.add("maxCompanyTotal", LongType, nullable = true)
			.add("minCompanyTotal", LongType, nullable = true)
			.add("avgCompanyTotal", DoubleType, nullable = true)
		// c. 应用schema到RDD上，创建DataFrame
		val aggDF: DataFrame = session.createDataFrame(rowRDD, schema)

		// 返回指标结果
		aggDF
	}

	def main(args: Array[String]): Unit = {
		execute(
			this.getClass, //
			OfflineTableDefine.WAREHOUSE_DETAIL, //
			OfflineTableDefine.WAREHOUSE_SUMMARY, //
			isLoadFull = Configuration.IS_FIRST_RUNNABLE //
		)
	}
}

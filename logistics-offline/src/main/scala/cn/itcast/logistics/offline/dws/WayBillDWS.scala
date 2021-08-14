package cn.itcast.logistics.offline.dws

import cn.itcast.logistics.common.{Configuration, OfflineTableDefine}
import cn.itcast.logistics.offline.AbstractOfflineApp
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructType}
import org.apache.spark.storage.StorageLevel

object WayBillDWS extends AbstractOfflineApp {
  /**
   * 按照业务指标，对宽表数据进行指标计算
   */
  override def process(dataframe: DataFrame): DataFrame = {
    // dataframe 数据集为宽表数据，运单宽表
    val session = dataframe.sparkSession
    import session.implicits._

    // 第一步、获取有多少天day

    val days: Array[Row] = dataframe.select($"day").distinct().collect()


    // 第二步、按照day划分数据，计算指标
    val rows: Array[Row] = days.map { dayRow: Row =>
      // 获取每天day值
      val dayValue: String = dayRow.getString(0)


      // 过滤获取每天运单数据
      val wayBillDetailDF: DataFrame = dataframe.where($"day".equalTo(dayValue))

      // 指标计算
      // 其一、注册DataFrame为临时视图

      wayBillDetailDF.createOrReplaceTempView("tmp_view_waybill")
      // TODO: 缓存SparkSQL中注册临时视图数据
      session.catalog.cacheTable("tmp_view_waybill", StorageLevel.MEMORY_AND_DISK)


      // 指标一：总运单数
      //wayBillDetailDF.agg(count("id").as("total"))
      val totalDF: DataFrame = session.sql(
        """
				  |SELECT COUNT(1) AS total FROM tmp_view_waybill
				  |""".stripMargin)

      // 指标二：各区域运单数，最大、最小和平均
      /*
      val areaTotalDF: DataFrame = wayBillDetailDF.groupBy($"area_id").count()
      val areaTotalAggDF: DataFrame = areaTotalDF.agg(
        max($"count").as("areaMaxTotal"),
        min($"count").as("areaMinTotal"),
        round(avg($"count"), 0).as("areaAvgTotal")
      )
       */
      val areaTotalAggDF: DataFrame = session.sql(
        """
				  |WITH tmp AS (
				  |  SELECT area_id, COUNT(1) AS total FROM tmp_view_waybill GROUP BY area_id
				  |)
				  |SELECT
				  |  max(total) AS areaMaxTotal,
				  |  min(total) AS areaMinTotal,
				  |  round(avg(total), 0) AS areaAvgTotal
				  |FROM tmp
				  |""".stripMargin)


      // 指标三：各分公司运单数，最大、最小和平均
      val companyTotalAggDF: DataFrame = session.sql(
        """
				  |WITH tmp AS (
				  |  SELECT sw_company_name, COUNT(1) AS total FROM tmp_view_waybill GROUP BY sw_company_name
				  |)
				  |SELECT
				  |  max(total) AS companyMaxTotal,
				  |  min(total) AS companyMinTotal,
				  |  round(avg(total), 0) AS companyAvgTotal
				  |FROM tmp
				  |""".stripMargin)

      // 指标四：各网点运单数，最大、最小和平均
      val dotTotalAggDF: DataFrame = session.sql(
        """
				  |WITH tmp AS (
				  |  SELECT dot_id, COUNT(1) AS total FROM tmp_view_waybill GROUP BY dot_id
				  |)
				  |SELECT
				  |  max(total) AS dotMaxTotal,
				  |  min(total) AS dotMinTotal,
				  |  round(avg(total), 0) AS dotAvgTotal
				  |FROM tmp
				  |""".stripMargin)

      // 指标五：各线路运单数，最大、最小和平均
      val routeTotalAggDF: DataFrame = session.sql(
        """
				  |WITH tmp AS (
				  |  SELECT route_id, COUNT(1) AS total FROM tmp_view_waybill GROUP BY route_id
				  |)
				  |SELECT
				  |  max(total) AS routeMaxTotal,
				  |  min(total) AS routeMinTotal,
				  |  round(avg(total), 0) AS routeAvgTotal
				  |FROM tmp
				  |""".stripMargin)

      // 指标六：各运输工具运单数，最大、最小和平均
      val ttTotalAggDF: DataFrame = session.sql(
        """
				  |WITH tmp AS (
				  |  SELECT tt_id, COUNT(1) AS total FROM tmp_view_waybill GROUP BY tt_id
				  |)
				  |SELECT
				  |  max(total) AS ttMaxTotal,
				  |  min(total) AS ttMinTotal,
				  |  round(avg(total), 0) AS ttAvgTotal
				  |FROM tmp
				  |""".stripMargin)

      // 指标七：各类客户运单数，最大、最小和平均
      val typeTotalAggDF: DataFrame = session.sql(
        """
				  |WITH tmp AS (
				  |  SELECT ctype, COUNT(1) AS total FROM tmp_view_waybill GROUP BY ctype
				  |)
				  |SELECT
				  |  max(total) AS typeMaxTotal,
				  |  min(total) AS typeMinTotal,
				  |  round(avg(total), 0) AS typeAvgTotal
				  |FROM tmp
				  |""".stripMargin)

      // 当数据不再使用，释放资源
      session.catalog.uncacheTable("tmp_view_waybill")
      // 封装各个指标到Row对象


      Row.fromSeq(
        dayRow.toSeq ++
          totalDF.first().toSeq ++
          areaTotalAggDF.first().toSeq ++
          companyTotalAggDF.first().toSeq ++
          dotTotalAggDF.first().toSeq ++
          routeTotalAggDF.first().toSeq ++
          ttTotalAggDF.first().toSeq ++
          typeTotalAggDF.first().toSeq
      )
    }

    // 第三步、封装指标结果到DataFrame数据集
    /*
      采用自定义Schema方式，转换数组Array为DataFrame数据集
      1. 并行化方式Array为RDD
      2. 自定义Schema信息，一定要与Row中字段对应上，尤其是数据类型
      3. 使用SparkSession中createDataFrame方法，构建数据集
     */
    // 1. 并行化方式Array为RDD
    val rowRDD: RDD[Row] = session.sparkContext.parallelize(rows, numSlices = 1)
    // 2. 自定义Schema信息，一定要与Row中字段对应上，尤其是数据类型
    val schema: StructType = new StructType()
      .add("id", StringType, nullable = false)
      .add("total", LongType, nullable = true)
      .add("maxAreaTotal", LongType, nullable = true)
      .add("minAreaTotal", LongType, nullable = true)
      .add("avgAreaTotal", DoubleType, nullable = true)
      .add("maxCompanyTotal", LongType, nullable = true)
      .add("minCompanyTotal", LongType, nullable = true)
      .add("avgCompanyTotal", DoubleType, nullable = true)
      .add("maxDotTotal", LongType, nullable = true)
      .add("minDotTotal", LongType, nullable = true)
      .add("avgDotTotal", DoubleType, nullable = true)
      .add("maxRouteTotal", LongType, nullable = true)
      .add("minRouteTotal", LongType, nullable = true)
      .add("avgRouteTotal", DoubleType, nullable = true)
      .add("maxToolTotal", LongType, nullable = true)
      .add("minToolTotal", LongType, nullable = true)
      .add("avgToolTotal", DoubleType, nullable = true)
      .add("maxCtypeTotal", LongType, nullable = true)
      .add("minCtypeTotal", LongType, nullable = true)
      .add("avgCtypeTotal", DoubleType, nullable = true)
    // 3. 使用SparkSession中createDataFrame方法，构建数据集
    val aggDF: DataFrame = session.createDataFrame(rowRDD, schema)

    // 返回指标结果
    aggDF
  }

  // TODO: 仅仅需要调用模板方法，传递参数即可
  def main(args: Array[String]): Unit = {
    execute(
      this.getClass, OfflineTableDefine.WAY_BILL_DETAIL, //
      OfflineTableDefine.WAY_BILL_SUMMARY, isLoadFull = Configuration.IS_FIRST_RUNNABLE //
    )
  }
}

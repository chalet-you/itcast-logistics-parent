package cn.itcast.logistics.offline.dws

import cn.itcast.logistics.common.{Configuration, OfflineTableDefine, SparkUtils}
import cn.itcast.logistics.offline.BasicOfflineApp
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructType}

/**
 * 快递单主题开发：
 * 加载Kudu中快递单宽表：tbl_express_bill_detail 数据，按照业务进行指标统计
 */
object ExpressBillDWS extends BasicOfflineApp {
  /**
   * 数据处理，此处是DWS层，业务指标计算
   */
  override def process(dataframe: DataFrame): DataFrame = {

    // 此处dataframe数据集：宽表数据
    val spark: SparkSession = dataframe.sparkSession
    import spark.implicits._

    /*
        如果加载全量数据，按照day日期划分数据进行每日快递单数据指标统计
        如果是增量数据，也就是昨日数据，直接计算即可
        TODO：无论是全量数据还是增量数据，直接按照全量数据处理，首先获取数据中day值，按照day划分数据，每天数据指标计算
    */
    // 第一步、获取day值（有多少天数据）
    val days: Array[Row] = dataframe.select($"day").distinct().collect()

    // 第二步、遍历days，按照日期day划分数据，对每天数据进行指标计算
    val aggRows: Array[Row] = days.map { dayRow: Row =>
      // a. 获取day值
      val dayValue: String = dayRow.getString(0)

      // b. 过滤每天day宽表数据
      val expressBillDetailDF: Dataset[Row] = dataframe.filter($"day" === dayValue)

      // c. 指标计算
      // 指标一：总快递单数
      //expressBillDetailDF.count()
      val totalDF: DataFrame = expressBillDetailDF.agg(count($"id").as("total"))

      // 指标二：各类客户快递单数，最大、最小和平均
      val typeTotalDF: DataFrame = expressBillDetailDF.groupBy($"type").count()
      val typeTotalAggDF: DataFrame = typeTotalDF.agg(
        max($"count").as("maxTypeTotal"), //
        min($"count").as("minTypeTotal"), //
        round(avg($"count"), 0).as("avgTypeTotal") //
      )

      // 指标三：各网点快递单数，最大、最小和平均
      val dotTotalDF: DataFrame = expressBillDetailDF.groupBy($"dot_id").count()
      val dotTotalAggDF: DataFrame = dotTotalDF.agg(
        max($"count").as("maxDotTotal"), //
        min($"count").as("minDotTotal"), //
        round(avg($"count"), 0).as("avgDotTotal") //
      )

      // 指标四：各渠道快递单数，最大、最小和平均
      val channelTotalDF: DataFrame = expressBillDetailDF.groupBy($"order_channel_id").count()
      val channelTotalAggDF: DataFrame = channelTotalDF.agg(
        max($"count").as("maxChannelTotal"), //
        min($"count").as("minChannelTotal"), //
        round(avg($"count"), 0).as("avgChannelTotal") //
      )

      // 指标五：各终端快递单数，最大、最小和平均
      val terminalTotalDF: DataFrame = expressBillDetailDF.groupBy($"order_terminal_type").count()
      val terminalTotalAggDF: DataFrame = terminalTotalDF.agg(
        max($"count").as("maxTerminalTotal"), //
        min($"count").as("minTerminalTotal"), //
        round(avg($"count"), 0).as("avgTerminalTotal") //
      )

      // 组合指标，封装到Row中
      val aggRow: Row = Row.fromSeq(
        Seq(dayValue) ++
          totalDF.first().toSeq ++
          typeTotalAggDF.first().toSeq ++
          dotTotalAggDF.first().toSeq ++
          channelTotalAggDF.first().toSeq ++
          terminalTotalAggDF.first().toSeq
      )

      // 返回每天计算指标
      aggRow
    }

    // 第三步、封装指标结果到DataFrame中
    // a. 将数组Array转换为RDD
    val rowRDD: RDD[Row] = spark.sparkContext.parallelize(aggRows)
    // b. 自定义Schema信息
    val schema = new StructType()
      .add("id", StringType, nullable = false)
      .add("total", LongType, nullable = true)
      .add("maxTypeTotal", LongType, nullable = true)
      .add("minTypeTotal", LongType, nullable = true)
      .add("avgTypeTotal", DoubleType, nullable = true)
      .add("maxDotTotal", LongType, nullable = true)
      .add("minDotTotal", LongType, nullable = true)
      .add("avgDotTotal", DoubleType, nullable = true)
      .add("maxChannelTotal", LongType, nullable = true)
      .add("minChannelTotal", LongType, nullable = true)
      .add("avgChannelTotal", DoubleType, nullable = true)
      .add("maxTerminalTotal", LongType, nullable = true)
      .add("minTerminalTotal", LongType, nullable = true)
      .add("avgTerminalTotal", DoubleType, nullable = true)

    // c. 创建DataFrame
    val aggDF: DataFrame = spark.createDataFrame(rowRDD, schema)

    // 第四步、返回计算指标数据集DataFrame
    aggDF
  }

  // SparkSQL应用程序入口，与DWD层程序类似
  def main(args: Array[String]): Unit = {
    // step1. 创建SparkSession对象，传递SparkConf对象
    val spark: SparkSession = SparkUtils.createSparkSession(
      SparkUtils.autoSettingEnv(SparkUtils.sparkConf()), this.getClass //
    )
    import spark.implicits._

    // step2. 加载Kudu中的事实表数据
    val kuduDF: DataFrame = load(spark, OfflineTableDefine.EXPRESS_BILL_DETAIL, isLoadFullData = Configuration.IS_FIRST_RUNNABLE)
    kuduDF.printSchema()
    kuduDF.show(10, truncate = false)

    // step3. 加载维度表数据，与事实表进行关联
    val etlDF: DataFrame = process(kuduDF)
    etlDF.printSchema()
    etlDF.show(10, truncate = false)

    // step4. 将拉宽后的数据再次写回到Kudu数据库中
    save(etlDF, OfflineTableDefine.EXPRESS_BILL_SUMMARY)

    // 应用结束，关闭资源
    spark.stop()
  }
}

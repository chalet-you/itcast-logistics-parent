package cn.itcast.logistics.offline

import cn.itcast.logistics.common.{Configuration, KuduTools}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

/**
 * 根据不同的主题开发，定义抽象方法
 *- 1. 数据读取load：从Kudu数据库的ODS层读取数据，事实表和维度表
 *- 2. 数据处理process：要么是拉链关联宽表，要么是依据业务指标分析得到结果表
 *- 3. 数据保存save：将宽表或结果表存储Kudu数据库的DWD层或者DWS层
 */
trait BasicOfflineApp {

  /**
   * 读取Kudu表的数据，依据指定Kudu表名称
   *
   * @param spark          SparkSession实例对象
   * @param tableName      表的名
   * @param isLoadFullData 是否加载全量数据，默认值为false
   */
  def load(spark: SparkSession, tableName: String, isLoadFullData: Boolean = false): DataFrame = {
    // a. 加载Kudu表数据，批量加载
    val kuduDF: DataFrame = spark.read
      .format(Configuration.SPARK_KUDU_FORMAT)
      .option("kudu.master", Configuration.KUDU_RPC_ADDRESS)
      .option("kudu.table", tableName)
      .load()

    // b. 判断是否全量，如果是，直接返回数据；否则，获取昨日数据
    if (isLoadFullData) {
      kuduDF
    } else {
      // 加载数据不是全量，就是增量，昨日数据，按照日期时间过滤
      kuduDF.filter(
        // cdt -> 2020-02-14 18:19:00  提取出日期 2020-02-14
        substring(col("cdt"), 0, 10) === date_sub(current_date(), 1).cast(StringType)
      )
    }
  }

  /**
   * 数据处理，如果是DWD层，事实表与维度表拉宽操作；如果是DWS层，业务指标计算
   */
  def process(dataframe: DataFrame): DataFrame

  /**
   * 数据存储: DWD及DWS层的数据都是需要写入到kudu数据库中，写入逻辑相同
   *
   * @param dataframe         数据集，主题指标结果数据
   * @param tableName         Kudu表的名称
   * @param isAutoCreateTable 是否自动创建表，默认为true，当表不存在时创建表
   */
  def save(dataframe: DataFrame, tableName: String, isAutoCreateTable: Boolean = true): Unit = {
    // a. 判断表是否运行创建，允许的话，先创建表
    if (isAutoCreateTable) {
      KuduTools.createKuduTable(tableName, dataframe) // 每个Kudu表主键列：id
    }

    // b. 保存数据到Kudu表
    dataframe.write
      .mode(SaveMode.Append)
      .format(Configuration.SPARK_KUDU_FORMAT)
      .option("kudu.master", Configuration.KUDU_RPC_ADDRESS)
      .option("kudu.table", tableName)
      .option("kudu.operation", "upsert")
      .save()
  }

}
package cn.itcast.logistics.offline

import cn.itcast.logistics.common.{Configuration, KuduTools, SparkUtils}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, current_date, date_sub, substring}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

trait AbstractOfflineApp {

  // 定义变量
  private var spark: SparkSession = _

  // 实例化spark对象
  def init(clazz: Class[_]): Unit = {
    // i. 获取SparkConf对象
    var sparkConf: SparkConf = SparkUtils.sparkConf()
    // ii. 依据运行系统，设置运行模式
    sparkConf = SparkUtils.autoSettingEnv(sparkConf)
    // iii. 构建会话实例
    spark = SparkUtils.createSparkSession(sparkConf, clazz)
  }

  // 从Kudu表加载数据
  def loadKuduSource(tableName: String, isLoadFullData: Boolean = false): DataFrame = {
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

  // 处理数据，要么是数据拉宽，要么是指标计算
  def process(dataframe: DataFrame): DataFrame

  // 保存数据到Kudu表
  def saveKuduSink(dataframe: DataFrame, tableName: String,
                   isAutoCreateTable: Boolean = true, keys: Seq[String] = Seq("id")) = {
    // a. 判断表是否运行创建，允许的话，先创建表
    if (isAutoCreateTable) {
      KuduTools.createKuduTable(tableName, dataframe, keys) // 每个Kudu表主键列：id
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

  // 关闭会话实例对象
  def close(): Unit = {
    if (null != spark) spark.close()
  }

  // TODO： 定义模块方法，规定基本方法执行顺序
  def execute(clazz: Class[_],
              srcTable: String, dstTable: String,
              isLoadFull: Boolean = false, isAutoCreateTable: Boolean = true, keys: Seq[String] = Seq("id")): Unit = {
    // step1. 初始化
    init(clazz)
    try {
      // step2. 加载Kudu表数据
      val kuduDF: DataFrame = loadKuduSource(srcTable, isLoadFull)
      kuduDF.show(10, truncate = false)

      // step3. 处理数据
      val resultDF: DataFrame = process(kuduDF)
      resultDF.show(10, truncate = false)

      // step4. 保存数据
      saveKuduSink(resultDF, dstTable, isAutoCreateTable, keys)
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      // step5. 关闭资源
      close()
    }
  }

}

package cn.itcast.logistics.offline.dwd

import cn.itcast.logistics.common.{CodeTypeMapping, Configuration, OfflineTableDefine, SparkUtils, TableMapping}
import cn.itcast.logistics.offline.BasicOfflineApp
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/*
* 快递单主题开发：
	* 将快递单事实表的数据与相关维度表的数据进行关联JOIN，然后将拉宽后的数据写入到快递单宽表中
	*       采用DSL语义实现离线计算程序
	* 最终离线程序需要部署到服务器，每天定时执行（Azkaban定时调度）
*/
object ExpressBillDWD extends BasicOfflineApp {
  /**
   * 数据处理，此处为DWD层，将事实表数据与维度表进行关联join，选择字段，存储至Kudu表
   */
  override def process(dataframe: DataFrame): DataFrame = {
    // dataframe 数据集表示事实表数据，此处指的就是tbl_express_bill 表数据

    val spark: SparkSession = dataframe.sparkSession
    import spark.implicits._

    // 第一步、加载维度表数据，调用load方法
    // 1.1：加载快递员维度表的数据
    val courierDF: DataFrame = load(spark, TableMapping.COURIER, isLoadFullData = true)
    // 1.2：加载客户维度表的数据
    val customerDF: DataFrame = load(spark, TableMapping.CUSTOMER, isLoadFullData = true)
    // 1.3：加载物流码表的数据
    val codesDF: DataFrame = load(spark, TableMapping.CODES, isLoadFullData = true)
    // 1.4：客户地址关联表的数据
    val addressMapDF: DataFrame = load(spark, TableMapping.CONSUMER_ADDRESS_MAP, isLoadFullData = true)
    // 1.5：加载地址表的数据
    val addressDF: DataFrame = load(spark, TableMapping.ADDRESS, isLoadFullData = true)
    // 1.6：加载包裹表的数据
    val pkgDF: DataFrame = load(spark, TableMapping.PKG, isLoadFullData = true)
    // 1.7：加载网点表的数据
    val dotDF: DataFrame = load(spark, TableMapping.DOT, isLoadFullData = true)
    // 1.8：加载公司网点表的数据
    val companyDotMapDF: DataFrame = load(spark, TableMapping.COMPANY_DOT_MAP, isLoadFullData = true)
    // 1.9：加载公司表的数据
    val companyDF: DataFrame = load(spark, TableMapping.COMPANY, isLoadFullData = true)
    // 1.10：获取终端类型码表数据
    val orderTerminalTypeDF: DataFrame = codesDF
      .where($"type" === CodeTypeMapping.ORDER_TERMINAL_TYPE)
      .select(
        $"code".as("OrderTerminalTypeCode"),
        $"codeDesc".as("OrderTerminalTypeName")
      )
    // 1.11：获取下单渠道类型码表数据
    val orderChannelTypeDF: DataFrame = codesDF
      .where($"type" === CodeTypeMapping.ORDER_CHANNEL_TYPE)
      .select(
        $"code".as("OrderChannelTypeCode"),
        $"codeDesc".as("OrderChannelTypeName")
      )

    // 第二步、事实表与维度表关联，大表在左，小表在右，采用leftJoin
    val expressBillDF: DataFrame = dataframe
    val joinType: String = "left_outer"
    val joinDF: DataFrame = expressBillDF
      // 快递单表与快递员表进行关联
      .join(courierDF, expressBillDF("eid") === courierDF("id"), joinType)
      // 快递单表与客户表进行关联
      .join(customerDF, expressBillDF("cid") === customerDF("id"), joinType)
      // 下单渠道表与快递单表关联
      .join(
        orderChannelTypeDF,
        orderChannelTypeDF("OrderChannelTypeCode") === expressBillDF("orderChannelId"),
        joinType
      )
      // 终端类型表与快递单表关联
      .join(
        orderTerminalTypeDF,
        orderTerminalTypeDF("OrderTerminalTypeCode") === expressBillDF("orderTerminalType"),
        joinType
      )
      // 客户地址关联表与客户表关联
      .join(addressMapDF, addressMapDF("consumerId") === customerDF("id"), joinType)
      // 地址表与客户地址关联表关联
      .join(addressDF, addressDF("id") === addressMapDF("addressId"), joinType)
      // 包裹表与快递单表关联
      .join(pkgDF, pkgDF("pwBill") === expressBillDF("expressNumber"), joinType)
      // 网点表与包裹表关联
      .join(dotDF, dotDF("id") === pkgDF("pwDotId"), joinType)
      // 公司网点关联表与网点表关联
      .join(companyDotMapDF, companyDotMapDF("dotId") === dotDF("id"), joinType)
      // 公司网点关联表与公司表关联
      .join(companyDF, companyDF("id") === companyDotMapDF("companyId"), joinType)

    // 第三步、选择字段，并且添加字段day日期字段，表示此条数据属于那天，为了后续指标计算准备
    val expressBillDetailDF: DataFrame = joinDF
      .select(
        expressBillDF("id"), // 快递单id
        expressBillDF("expressNumber").as("express_number"), //快递单编号
        expressBillDF("cid"), //客户id
        customerDF("name").as("cname"), //客户名称
        addressDF("detailAddr").as("caddress"), //客户地址
        expressBillDF("eid"), //员工id
        courierDF("name").as("ename"), //员工名称
        dotDF("id").as("dot_id"), //网点id
        dotDF("dotName").as("dot_name"), //网点名称
        companyDF("companyName").as("company_name"), //公司名称
        expressBillDF("orderChannelId").as("order_channel_id"), //下单渠道id
        orderChannelTypeDF("OrderChannelTypeName").as("order_channel_name"), //下单渠道id
        expressBillDF("orderDt").as("order_dt"), //下单时间
        orderTerminalTypeDF("OrderTerminalTypeCode").as("order_terminal_type"), //下单设备类型id
        orderTerminalTypeDF("OrderTerminalTypeName").as("order_terminal_type_name"), //下单设备类型id
        expressBillDF("orderTerminalOsType").as("order_terminal_os_type"), //下单设备操作系统
        expressBillDF("reserveDt").as("reserve_dt"), //预约取件时间
        expressBillDF("isCollectPackageTimeout").as("is_collect_package_timeout"), //是否取件超时
        expressBillDF("timeoutDt").as("timeout_dt"), //超时时间
        customerDF("type"), //客户类型
        expressBillDF("cdt"), //创建时间
        expressBillDF("udt"), //修改时间
        expressBillDF("remark") //备注
      )
      .sort($"cdt".asc)
      // 添加字段：day 日期字段，标识每条数据所属那一天,  2020-02-14 18:19:00 -> 20200214
      .withColumn("day", regexp_replace(substring($"cdt", 0, 10), "-", ""))

    // 返回拉宽后数据
    expressBillDetailDF
  }

  /*
  数据处理，实现步骤：
    step1. 创建SparkSession对象，传递SparkConf对象
    step2. 加载Kudu中的事实表数据
    step3. 加载维度表数据，与事实表进行关联
    step4. 将拉宽后的数据再次写回到Kudu数据库中
  */
  def main(args: Array[String]): Unit = {
    // step1. 创建SparkSession对象，传递SparkConf对象
    val spark: SparkSession = SparkUtils.createSparkSession(
      SparkUtils.autoSettingEnv(SparkUtils.sparkConf()), this.getClass //
    )
    import spark.implicits._

    // step2. 加载Kudu中的事实表数据
    val kuduDF: DataFrame = load(spark, TableMapping.EXPRESS_BILL, isLoadFullData = Configuration.IS_FIRST_RUNNABLE)
    kuduDF.printSchema()
    kuduDF.show(10, truncate = false)

    // step3. 加载维度表数据，与事实表进行关联
    val etlDF: DataFrame = process(kuduDF)
    etlDF.printSchema()
    etlDF.show(10, truncate = false)

    // step4. 将拉宽后的数据再次写回到Kudu数据库中
    save(etlDF, OfflineTableDefine.EXPRESS_BILL_DETAIL)

    // 应用结束，关闭资源
    spark.stop()
  }

}

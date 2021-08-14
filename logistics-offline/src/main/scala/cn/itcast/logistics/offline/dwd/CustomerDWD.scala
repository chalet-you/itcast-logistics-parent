package cn.itcast.logistics.offline.dwd

import cn.itcast.logistics.common.{CodeTypeMapping, Configuration, OfflineTableDefine, TableMapping}
import cn.itcast.logistics.offline.AbstractOfflineApp
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, DecimalType, IntegerType}

/**
 * 客户主题拉链宽表：
 *      将Kudu中ODS层客户表：tbl_customer与相关维度表数据进行JOIN关联，拉链为宽表存储Kudu中DWD层。
 */
object CustomerDWD extends AbstractOfflineApp{
	// 数据拉宽操作
	override def process(dataframe: DataFrame): DataFrame = {
		import dataframe.sparkSession.implicits._
		
		// TODO: 1). 加载维度表的数据
		val customerSenderInfoDF: DataFrame = loadKuduSource(
			TableMapping.CONSUMER_SENDER_INFO, isLoadFullData = Configuration.IS_FIRST_RUNNABLE
		)
		val expressPackageDF: DataFrame = loadKuduSource(TableMapping.EXPRESS_PACKAGE, isLoadFullData = true)
		val codesDF: DataFrame = loadKuduSource(TableMapping.CODES, isLoadFullData = true)
		val customerTypeDF : DataFrame= codesDF.where($"type" === CodeTypeMapping.CUSTOM_TYPE)
		
		// TODO: step2. 定义维度表与事实表的关联
		val joinType: String = "left_outer"
		// 获取每个用户的首尾单发货信息及发货件数和总金额
		val customerSenderDetailInfoDF: DataFrame = customerSenderInfoDF
			.join(expressPackageDF, expressPackageDF("id") === customerSenderInfoDF("pkgId"), joinType)
			.groupBy(customerSenderInfoDF("ciid"))
			.agg(
				min(customerSenderInfoDF("id")).alias("first_id"), //
				max(customerSenderInfoDF("id")).alias("last_id"),  //
				min(expressPackageDF("cdt")).alias("first_cdt"),  //
				max(expressPackageDF("cdt")).alias("last_cdt"),  //
				count(customerSenderInfoDF("id")).alias("totalCount"),  //
				sum(expressPackageDF("actualAmount")).alias("totalAmount")  //
				// TODO: 对double类型数据求和时，必须转换为BigDecimal类型，否则精度丢失
				//sum(expressPackageDF("actualAmount").cast(DataTypes.createDecimalType(10, 2))).alias("totalAmount")  //
			)
		
		/**
		 * 不需要添加日期字段，针对所有用户进行指标计算
		 */
		val customerDF: DataFrame = dataframe
		val customerDetailDF: DataFrame = customerDF
			.join(customerSenderDetailInfoDF, customerDF("id") === customerSenderInfoDF("ciid"), joinType)
			.join(customerTypeDF, customerDF("type") === customerTypeDF("code").cast(IntegerType), joinType)
			.sort(customerDF("cdt").asc)
			.select(
				customerDF("id"),
				customerDF("name"),
				customerDF("tel"),
				customerDF("mobile"),
				customerDF("type").cast(IntegerType),
				customerTypeDF("codeDesc").as("type_name"),
				customerDF("isownreg").as("is_own_reg"),
				customerDF("regdt").as("regdt"),
				customerDF("regchannelid").as("reg_channel_id"),
				customerDF("state"),
				customerDF("cdt"),
				customerDF("udt"),
				customerDF("lastlogindt").as("last_login_dt"),
				customerDF("remark"),
				customerSenderDetailInfoDF("first_id").as("first_sender_id"), //首次寄件id
				customerSenderDetailInfoDF("last_id").as("last_sender_id"), //尾次寄件id
				customerSenderDetailInfoDF("first_cdt").as("first_sender_cdt"), //首次寄件时间
				customerSenderDetailInfoDF("last_cdt").as("last_sender_cdt"), //尾次寄件时间
				customerSenderDetailInfoDF("totalCount"), //寄件总次数
				customerSenderDetailInfoDF("totalAmount") //总金额
			)
		
		// 返回拉链后宽表数据
		customerDetailDF
	}
	
	def main(args: Array[String]): Unit = {
		execute(
			this.getClass, TableMapping.CUSTOMER ,//
			OfflineTableDefine.CUSTOMER_DETAIL, //
			isLoadFull = true // 全量数据加载
		)
	}
}

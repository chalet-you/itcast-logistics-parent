package cn.itcast.logistics.etl.realtime.kudu

import cn.itcast.logistics.common.BeanImplicits._
import cn.itcast.logistics.common.beans.crm.{AddressBean, ConsumerAddressMapBean, CustomerBean}
import cn.itcast.logistics.common.beans.logistics.AreasBean
import cn.itcast.logistics.common.beans.parser.{CanalMessageBean, OggMessageBean}
import cn.itcast.logistics.common.{SparkUtils, TableMapping}
import cn.itcast.logistics.etl.parser.DataParser
import cn.itcast.logistics.etl.realtime.BasicStreamApp
import com.alibaba.fastjson.JSON
import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object _07KuduStreamApp extends BasicStreamApp {
	
	/**
	 * 数据的处理，仅仅将JSON转换为MessageBean
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
					}
				// 返回转换后数据
				beanDS.toDF()
				
			// CRM 系统业务数据处理转换
			case "crm" =>
				// step1. JSON -> MessageBean
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
	
	/**
	 * 将OGG采集数据JSON转换为MessageBean对象后，按照业务系统表过滤，封装到对应POJO对象，并且保存到外部存储
	 *      第一、MessageBean -> POJO
	 *      第二、保存数据
	 */
	def etlLogistics(streamDF: DataFrame): Unit = {
		// 将DataFrame转换为Dataset
		val oggBeanStreamDS: Dataset[OggMessageBean] = streamDF.as[OggMessageBean]
		
		// TODO: 此处以表【tbl_areas】为例，将其数据封装到POJO对象中
		val pojoDS: Dataset[AreasBean] = oggBeanStreamDS
			// 依据表的名称，过滤获取相关数据 ->  "tbl_areas" == bean.getTable
			.filter(bean => TableMapping.AREAS.equals(bean.getTable))
			// 提取bean字段值（opType和getvalue）封装POJO对象
			.map(bean => DataParser.toAreaBean(bean))
			// 再次过滤，如果数据转换pojo对象失败，返回null值
			.filter(pojo => null != pojo)
		save(pojoDS.toDF(), TableMapping.AREAS)
		
		val warehouseSendVehicleStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.WAREHOUSE_SEND_VEHICLE)
			.map(bean => DataParser.toWarehouseSendVehicle(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(warehouseSendVehicleStreamDF, TableMapping.WAREHOUSE_SEND_VEHICLE)
		
		val waybillLineStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.WAYBILL_LINE)
			.map(bean => DataParser.toWaybillLine(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(waybillLineStreamDF, TableMapping.WAYBILL_LINE)
		
		val chargeStandardStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.CHARGE_STANDARD)
			.map(bean => DataParser.toChargeStandard(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(chargeStandardStreamDF, TableMapping.CHARGE_STANDARD)
		
		val codesStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.CODES)
			.map(bean => DataParser.toCodes(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(codesStreamDF, TableMapping.CODES)
		
		val collectPackageStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.COLLECT_PACKAGE)
			.map(bean => DataParser.toCollectPackage(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(collectPackageStreamDF, TableMapping.COLLECT_PACKAGE)
		
		val companyStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.COMPANY)
			.map(bean => DataParser.toCompany(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(companyStreamDF, TableMapping.COMPANY)
		
		val companyDotMapStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.COMPANY_DOT_MAP)
			.map(bean => DataParser.toCompanyDotMap(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(companyDotMapStreamDF, TableMapping.COMPANY_DOT_MAP)
		
		val companyTransportRouteMaStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.COMPANY_TRANSPORT_ROUTE_MA)
			.map(bean => DataParser.toCompanyTransportRouteMa(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(companyTransportRouteMaStreamDF, TableMapping.COMPANY_TRANSPORT_ROUTE_MA)
		
		val companyWarehouseMapStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.COMPANY_WAREHOUSE_MAP)
			.map(bean => DataParser.toCompanyWarehouseMap(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(companyWarehouseMapStreamDF, TableMapping.COMPANY_WAREHOUSE_MAP)
		
		val consumerSenderInfoStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.CONSUMER_SENDER_INFO)
			.map(bean => DataParser.toConsumerSenderInfo(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(consumerSenderInfoStreamDF, TableMapping.CONSUMER_SENDER_INFO)
		
		val courierStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.COURIER)
			.map(bean => DataParser.toCourier(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(courierStreamDF, TableMapping.COURIER)
		
		val deliverPackageStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.DELIVER_PACKAGE)
			.map(bean => DataParser.toDeliverPackage(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(deliverPackageStreamDF, TableMapping.DELIVER_PACKAGE)
		
		val deliverRegionStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.DELIVER_REGION)
			.map(bean => DataParser.toDeliverRegion(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(deliverRegionStreamDF, TableMapping.DELIVER_REGION)
		
		val deliveryRecordStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.DELIVERY_RECORD)
			.map(bean => DataParser.toDeliveryRecord(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(deliveryRecordStreamDF, TableMapping.DELIVERY_RECORD)
		
		val departmentStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.DEPARTMENT)
			.map(bean => DataParser.toDepartment(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(departmentStreamDF, TableMapping.DEPARTMENT)
		
		val dotStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.DOT)
			.map(bean => DataParser.toDot(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(dotStreamDF, TableMapping.DOT)
		
		val dotTransportToolStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.DOT_TRANSPORT_TOOL)
			.map(bean => DataParser.toDotTransportTool(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(dotTransportToolStreamDF, TableMapping.DOT_TRANSPORT_TOOL)
		
		val driverStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.DRIVER)
			.map(bean => DataParser.toDriver(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(driverStreamDF, TableMapping.DRIVER)
		
		val empStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.EMP)
			.map(bean => DataParser.toEmp(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(empStreamDF, TableMapping.EMP)
		
		val empInfoMapStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.EMP_INFO_MAP)
			.map(bean => DataParser.toEmpInfoMap(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(empInfoMapStreamDF, TableMapping.EMP_INFO_MAP)
		
		val expressBillStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.EXPRESS_BILL)
			.map(bean => DataParser.toExpressBill(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(expressBillStreamDF, TableMapping.EXPRESS_BILL)
		
		val expressPackageStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.EXPRESS_PACKAGE)
			.map(bean => DataParser.toExpressPackage(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(expressPackageStreamDF, TableMapping.EXPRESS_PACKAGE)
		
		val fixedAreaStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.FIXED_AREA)
			.map(bean => DataParser.toFixedArea(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(fixedAreaStreamDF, TableMapping.FIXED_AREA)
		
		val goodsRackStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.GOODS_RACK)
			.map(bean => DataParser.toGoodsRack(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(goodsRackStreamDF, TableMapping.GOODS_RACK)
		
		val jobStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.JOB)
			.map(bean => DataParser.toJob(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(jobStreamDF, TableMapping.JOB)
		
		val outWarehouseStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.OUT_WAREHOUSE)
			.map(bean => DataParser.toOutWarehouse(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(outWarehouseStreamDF, TableMapping.OUT_WAREHOUSE)
		
		val outWarehouseDetailStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.OUT_WAREHOUSE_DETAIL)
			.map(bean => DataParser.toOutWarehouseDetail(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(outWarehouseDetailStreamDF, TableMapping.OUT_WAREHOUSE_DETAIL)
		
		val pkgStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.PKG)
			.map(bean => DataParser.toPkg(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(pkgStreamDF, TableMapping.PKG)
		
		val postalStandardStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.POSTAL_STANDARD)
			.map(bean => DataParser.toPostalStandard(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(postalStandardStreamDF, TableMapping.POSTAL_STANDARD)
		
		val pushWarehouseStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.PUSH_WAREHOUSE)
			.map(bean => DataParser.toPushWarehouse(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(pushWarehouseStreamDF, TableMapping.PUSH_WAREHOUSE)
		
		val pushWarehouseDetailStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.PUSH_WAREHOUSE_DETAIL)
			.map(bean => DataParser.toPushWarehouseDetail(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(pushWarehouseDetailStreamDF, TableMapping.PUSH_WAREHOUSE_DETAIL)
		
		val routeStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.ROUTE)
			.map(bean => DataParser.toRoute(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(routeStreamDF, TableMapping.ROUTE)
		
		val serviceEvaluationStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.SERVICE_EVALUATION)
			.map(bean => DataParser.toServiceEvaluation(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(serviceEvaluationStreamDF, TableMapping.SERVICE_EVALUATION)
		
		val storeGridStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.STORE_GRID)
			.map(bean => DataParser.toStoreGrid(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(storeGridStreamDF, TableMapping.STORE_GRID)
		
		val transportToolStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.TRANSPORT_TOOL)
			.map(bean => DataParser.toTransportTool(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(transportToolStreamDF, TableMapping.TRANSPORT_TOOL)
		
		val vehicleMonitorStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.VEHICLE_MONITOR)
			.map(bean => DataParser.toVehicleMonitor(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(vehicleMonitorStreamDF, TableMapping.VEHICLE_MONITOR)
		
		val warehouseStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.WAREHOUSE)
			.map(bean => DataParser.toWarehouse(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(warehouseStreamDF, TableMapping.WAREHOUSE)
		
		val warehouseEmpStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.WAREHOUSE_EMP)
			.map(bean => DataParser.toWarehouseEmp(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(warehouseEmpStreamDF, TableMapping.WAREHOUSE_EMP)
		
		val warehouseRackMapStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.WAREHOUSE_RACK_MAP)
			.map(bean => DataParser.toWarehouseRackMap(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(warehouseRackMapStreamDF, TableMapping.WAREHOUSE_RACK_MAP)
		
		val warehouseReceiptStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.WAREHOUSE_RECEIPT)
			.map(bean => DataParser.toWarehouseReceipt(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(warehouseReceiptStreamDF, TableMapping.WAREHOUSE_RECEIPT)
		
		val warehouseReceiptDetailStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.WAREHOUSE_RECEIPT_DETAIL)
			.map(bean => DataParser.toWarehouseReceiptDetail(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(warehouseReceiptDetailStreamDF, TableMapping.WAREHOUSE_RECEIPT_DETAIL)
		
		val warehouseTransportToolStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.WAREHOUSE_TRANSPORT_TOOL)
			.map(bean => DataParser.toWarehouseTransportTool(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(warehouseTransportToolStreamDF, TableMapping.WAREHOUSE_TRANSPORT_TOOL)
		
		val warehouseVehicleMapStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.WAREHOUSE_VEHICLE_MAP)
			.map(bean => DataParser.toWarehouseVehicleMap(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(warehouseVehicleMapStreamDF, TableMapping.WAREHOUSE_VEHICLE_MAP)
		
		val waybillStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.WAY_BILL)
			.map(bean => DataParser.toWaybill(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(waybillStreamDF, TableMapping.WAY_BILL)
		
		val waybillStateRecordStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.WAYBILL_STATE_RECORD)
			.map(bean => DataParser.toWaybillStateRecord(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(waybillStateRecordStreamDF, TableMapping.WAYBILL_STATE_RECORD)
		
		val workTimeStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.WORK_TIME)
			.map(bean => DataParser.toWorkTime(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(workTimeStreamDF, TableMapping.WORK_TIME)
		
		val transportRecordStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.TRANSPORT_RECORD)
			.map(bean => DataParser.toTransportRecordBean(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(transportRecordStreamDF, TableMapping.TRANSPORT_RECORD)
	}
	
	/**
	 * 将Canal采集数据JSON转换为MessageBean对象后，按照业务系统表过滤，封装到对应POJO对象，并且保存到外部存储
	 *      第一、MessageBean -> POJO
	 *      第二、保存数据
	 */
	def etlCrm(streamDF: DataFrame): Unit = {
		
		// 转换DataFrame为Dataset
		val canalBeanStreamDS: Dataset[CanalMessageBean] = streamDF.as[CanalMessageBean]
		
		// TODO: 此处以表【crm_address】为例，将其数据封装到POJO对象中
		val pojoDS: Dataset[AddressBean] = canalBeanStreamDS
			// 依据表的名称，过滤获取对应数据
			.filter(bean => TableMapping.ADDRESS.equals(bean.getTable))
			// 提取字段值，封装到对应表POJO对象
			.flatMap(bean => DataParser.toAddressBean(bean))
			// 过滤null数据（解析转换失败）
			.filter(pojo => null != pojo)
		save(pojoDS.toDF(), TableMapping.ADDRESS)
		
		// Customer 表数据
		val customerStreamDS: Dataset[CustomerBean] = canalBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.CUSTOMER)
			.map(bean => DataParser.toCustomer(bean))
			.filter( pojo => null != pojo)
		save(customerStreamDS.toDF(), TableMapping.CUSTOMER)
		// ConsumerAddressMap 表数据
		val consumerAddressMapStreamDS: Dataset[ConsumerAddressMapBean] = canalBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.CONSUMER_ADDRESS_MAP)
			.map(bean => DataParser.toConsumerAddressMap(bean))
			.filter( pojo => null != pojo)
		save(consumerAddressMapStreamDS.toDF(), TableMapping.CONSUMER_ADDRESS_MAP)
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
		val logisticsStreamDF: DataFrame = load(spark, "logistics")
		val crmStreamDF: DataFrame = load(spark, "crm")
		
		// step3. 对获取Json数据进行ETL转换(json -> bean)
		val logisticsEtlStreamDF = process(logisticsStreamDF, "logistics")
		val crmEtlStreamDF: DataFrame = process(crmStreamDF, "crm")
		
		// step4. 保存转换后数据到外部存储(bean-> pojo和save）
		etlLogistics(logisticsEtlStreamDF)
		etlCrm(crmEtlStreamDF)

		// step5. 应用启动以后，等待终止结束
		spark.streams.active.foreach(query => println(s"Query: ${query.name} is Running ............"))
		spark.streams.awaitAnyTermination()
	}
}

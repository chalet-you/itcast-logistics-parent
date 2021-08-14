package cn.itcast.logistics.etl.realtime

import cn.itcast.logistics.common.beans.logistics.{AreasBean, ChargeStandardBean, CodesBean, CollectPackageBean, CompanyBean, CompanyDotMapBean, CompanyTransportRouteMaBean, CompanyWarehouseMapBean, ConsumerSenderInfoBean, CourierBean, DeliverPackageBean, DeliverRegionBean, DeliveryRecordBean, DepartmentBean, DotBean, DotTransportToolBean, DriverBean, EmpBean, EmpInfoMapBean, ExpressBillBean, ExpressPackageBean, FixedAreaBean, GoodsRackBean, JobBean, OutWarehouseBean, OutWarehouseDetailBean, PkgBean, PostalStandardBean, PushWarehouseBean, PushWarehouseDetailBean, RouteBean, ServiceEvaluationBean, StoreGridBean, TransportRecordBean, TransportToolBean, VehicleMonitorBean, WarehouseBean, WarehouseEmpBean, WarehouseRackMapBean, WarehouseReceiptBean, WarehouseReceiptDetailBean, WarehouseSendVehicleBean, WarehouseTransportToolBean, WarehouseVehicleMapBean, WaybillBean, WaybillLineBean, WaybillStateRecordBean, WorkTimeBean}
import cn.itcast.logistics.common.{Configuration, KuduTools, SparkUtils, TableMapping}
import cn.itcast.logistics.common.BeanImplicits._
import cn.itcast.logistics.common.beans.crm.{AddressBean, ConsumerAddressMapBean, CustomerBean}
import cn.itcast.logistics.common.beans.parser.{CanalMessageBean, OggMessageBean}
import cn.itcast.logistics.etl.parser.DataParser
import com.alibaba.fastjson.JSON
import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SparkSession}

object KuduStreamApp extends BasicStreamApp {
	
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
					.filter((message: String) => null != message && message.trim.length > 0)
					// 解析每条数据
					.map { message: String =>
						JSON.parseObject(message.trim, classOf[OggMessageBean])
					}
				// 返回转换后数据
				beanDS.toDF()
				
			// CRM 系统业务数据处理转换
			case "crm" =>
				// step1. JSON -> MessageBean
				val beanDS: Dataset[CanalMessageBean] = streamDF
					// 过滤数据
					.filter((row: Row) => !row.isNullAt(0))
					// 解析数据
					.map { row: Row =>
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
		
		import streamDF.sparkSession.implicits._
		
		// step1、当表不存在时，如果允许创建表，先创建表，再插入数据
		if(isAutoCreateTable){
			KuduTools.createKuduTable(tableName, streamDF.drop("opType"))
		}
		
		// step2. 保存ETL转换后的数据
		/*
			依据OpType字段值，将数据“保存”到Kudu表中：
			- opType字段：insert 或者update时，插入更新数据到Kudu表
				kudu.operation=upsert
			- opType字段：delete时，按照主键删除Kudu表数据
				kudu.operation=delete
		 */
		// TODO: 过滤获取insert和update数据，进行插入更新操作
		streamDF
			.filter($"opType".equalTo("insert") || $"opType".equalTo("update"))
    		.drop($"opType")
			.writeStream
			.outputMode(OutputMode.Append())
			.queryName(s"query-${Configuration.SPARK_KUDU_FORMAT}-upsert-${tableName}")
			.format("kudu")
			.option("kudu.master", Configuration.KUDU_RPC_ADDRESS)
			.option("kudu.table", tableName)
			.option("kudu.operation", "upsert")
			.start()
		
		// TODO: 过滤获取delete数据，进行删除操作
		streamDF
			.filter($"opType".equalTo("delete"))
    		.select($"id")
			.writeStream
			.outputMode(OutputMode.Append())
			.queryName(s"query-${Configuration.SPARK_KUDU_FORMAT}-delete-${tableName}")
			.format("kudu")
			.option("kudu.master", Configuration.KUDU_RPC_ADDRESS)
			.option("kudu.table", tableName)
			.option("kudu.operation", "delete")
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
			.filter((bean: OggMessageBean) => TableMapping.AREAS.equals(bean.getTable))
			// 提取bean字段值（opType和getvalue）封装POJO对象
			.map((bean: OggMessageBean) => DataParser.toAreaBean(bean))
			// 再次过滤，如果数据转换pojo对象失败，返回null值
			.filter((pojo: AreasBean) => null != pojo)
		save(pojoDS.toDF(), TableMapping.AREAS)
		
		val warehouseSendVehicleStreamDF: DataFrame = oggBeanStreamDS
			.filter((bean: OggMessageBean) => bean.getTable == TableMapping.WAREHOUSE_SEND_VEHICLE)
			.map((bean: OggMessageBean) => DataParser.toWarehouseSendVehicle(bean))
			.filter((pojo: WarehouseSendVehicleBean) => null != pojo)
			.toDF()
		save(warehouseSendVehicleStreamDF, TableMapping.WAREHOUSE_SEND_VEHICLE)
		
		val waybillLineStreamDF: DataFrame = oggBeanStreamDS
			.filter((bean: OggMessageBean) => bean.getTable == TableMapping.WAYBILL_LINE)
			.map((bean: OggMessageBean) => DataParser.toWaybillLine(bean))
			.filter((pojo: WaybillLineBean) => null != pojo)
			.toDF()
		save(waybillLineStreamDF, TableMapping.WAYBILL_LINE)
		
		val chargeStandardStreamDF: DataFrame = oggBeanStreamDS
			.filter((bean: OggMessageBean) => bean.getTable == TableMapping.CHARGE_STANDARD)
			.map((bean: OggMessageBean) => DataParser.toChargeStandard(bean))
			.filter((pojo: ChargeStandardBean) => null != pojo)
			.toDF()
		save(chargeStandardStreamDF, TableMapping.CHARGE_STANDARD)
		
		val codesStreamDF: DataFrame = oggBeanStreamDS
			.filter((bean: OggMessageBean) => bean.getTable == TableMapping.CODES)
			.map((bean: OggMessageBean) => DataParser.toCodes(bean))
			.filter((pojo: CodesBean) => null != pojo)
			.toDF()
		save(codesStreamDF, TableMapping.CODES)
		
		val collectPackageStreamDF: DataFrame = oggBeanStreamDS
			.filter((bean: OggMessageBean) => bean.getTable == TableMapping.COLLECT_PACKAGE)
			.map((bean: OggMessageBean) => DataParser.toCollectPackage(bean))
			.filter((pojo: CollectPackageBean) => null != pojo)
			.toDF()
		save(collectPackageStreamDF, TableMapping.COLLECT_PACKAGE)
		
		val companyStreamDF: DataFrame = oggBeanStreamDS
			.filter((bean: OggMessageBean) => bean.getTable == TableMapping.COMPANY)
			.map((bean: OggMessageBean) => DataParser.toCompany(bean))
			.filter((pojo: CompanyBean) => null != pojo)
			.toDF()
		save(companyStreamDF, TableMapping.COMPANY)
		
		val companyDotMapStreamDF: DataFrame = oggBeanStreamDS
			.filter((bean: OggMessageBean) => bean.getTable == TableMapping.COMPANY_DOT_MAP)
			.map((bean: OggMessageBean) => DataParser.toCompanyDotMap(bean))
			.filter((pojo: CompanyDotMapBean) => null != pojo)
			.toDF()
		save(companyDotMapStreamDF, TableMapping.COMPANY_DOT_MAP)
		
		val companyTransportRouteMaStreamDF: DataFrame = oggBeanStreamDS
			.filter((bean: OggMessageBean) => bean.getTable == TableMapping.COMPANY_TRANSPORT_ROUTE_MA)
			.map((bean: OggMessageBean) => DataParser.toCompanyTransportRouteMa(bean))
			.filter((pojo: CompanyTransportRouteMaBean) => null != pojo)
			.toDF()
		save(companyTransportRouteMaStreamDF, TableMapping.COMPANY_TRANSPORT_ROUTE_MA)
		
		val companyWarehouseMapStreamDF: DataFrame = oggBeanStreamDS
			.filter((bean: OggMessageBean) => bean.getTable == TableMapping.COMPANY_WAREHOUSE_MAP)
			.map((bean: OggMessageBean) => DataParser.toCompanyWarehouseMap(bean))
			.filter((pojo: CompanyWarehouseMapBean) => null != pojo)
			.toDF()
		save(companyWarehouseMapStreamDF, TableMapping.COMPANY_WAREHOUSE_MAP)
		
		val consumerSenderInfoStreamDF: DataFrame = oggBeanStreamDS
			.filter((bean: OggMessageBean) => bean.getTable == TableMapping.CONSUMER_SENDER_INFO)
			.map((bean: OggMessageBean) => DataParser.toConsumerSenderInfo(bean))
			.filter((pojo: ConsumerSenderInfoBean) => null != pojo)
			.toDF()
		save(consumerSenderInfoStreamDF, TableMapping.CONSUMER_SENDER_INFO)
		
		val courierStreamDF: DataFrame = oggBeanStreamDS
			.filter((bean: OggMessageBean) => bean.getTable == TableMapping.COURIER)
			.map((bean: OggMessageBean) => DataParser.toCourier(bean))
			.filter((pojo: CourierBean) => null != pojo)
			.toDF()
		save(courierStreamDF, TableMapping.COURIER)
		
		val deliverPackageStreamDF: DataFrame = oggBeanStreamDS
			.filter((bean: OggMessageBean) => bean.getTable == TableMapping.DELIVER_PACKAGE)
			.map((bean: OggMessageBean) => DataParser.toDeliverPackage(bean))
			.filter((pojo: DeliverPackageBean) => null != pojo)
			.toDF()
		save(deliverPackageStreamDF, TableMapping.DELIVER_PACKAGE)
		
		val deliverRegionStreamDF: DataFrame = oggBeanStreamDS
			.filter((bean: OggMessageBean) => bean.getTable == TableMapping.DELIVER_REGION)
			.map((bean: OggMessageBean) => DataParser.toDeliverRegion(bean))
			.filter((pojo: DeliverRegionBean) => null != pojo)
			.toDF()
		save(deliverRegionStreamDF, TableMapping.DELIVER_REGION)
		
		val deliveryRecordStreamDF: DataFrame = oggBeanStreamDS
			.filter((bean: OggMessageBean) => bean.getTable == TableMapping.DELIVERY_RECORD)
			.map((bean: OggMessageBean) => DataParser.toDeliveryRecord(bean))
			.filter((pojo: DeliveryRecordBean) => null != pojo)
			.toDF()
		save(deliveryRecordStreamDF, TableMapping.DELIVERY_RECORD)
		
		val departmentStreamDF: DataFrame = oggBeanStreamDS
			.filter((bean: OggMessageBean) => bean.getTable == TableMapping.DEPARTMENT)
			.map((bean: OggMessageBean) => DataParser.toDepartment(bean))
			.filter((pojo: DepartmentBean) => null != pojo)
			.toDF()
		save(departmentStreamDF, TableMapping.DEPARTMENT)
		
		val dotStreamDF: DataFrame = oggBeanStreamDS
			.filter((bean: OggMessageBean) => bean.getTable == TableMapping.DOT)
			.map((bean: OggMessageBean) => DataParser.toDot(bean))
			.filter((pojo: DotBean) => null != pojo)
			.toDF()
		save(dotStreamDF, TableMapping.DOT)
		
		val dotTransportToolStreamDF: DataFrame = oggBeanStreamDS
			.filter((bean: OggMessageBean) => bean.getTable == TableMapping.DOT_TRANSPORT_TOOL)
			.map((bean: OggMessageBean) => DataParser.toDotTransportTool(bean))
			.filter((pojo: DotTransportToolBean) => null != pojo)
			.toDF()
		save(dotTransportToolStreamDF, TableMapping.DOT_TRANSPORT_TOOL)
		
		val driverStreamDF: DataFrame = oggBeanStreamDS
			.filter((bean: OggMessageBean) => bean.getTable == TableMapping.DRIVER)
			.map((bean: OggMessageBean) => DataParser.toDriver(bean))
			.filter((pojo: DriverBean) => null != pojo)
			.toDF()
		save(driverStreamDF, TableMapping.DRIVER)
		
		val empStreamDF: DataFrame = oggBeanStreamDS
			.filter((bean: OggMessageBean) => bean.getTable == TableMapping.EMP)
			.map((bean: OggMessageBean) => DataParser.toEmp(bean))
			.filter((pojo: EmpBean) => null != pojo)
			.toDF()
		save(empStreamDF, TableMapping.EMP)
		
		val empInfoMapStreamDF: DataFrame = oggBeanStreamDS
			.filter((bean: OggMessageBean) => bean.getTable == TableMapping.EMP_INFO_MAP)
			.map((bean: OggMessageBean) => DataParser.toEmpInfoMap(bean))
			.filter((pojo: EmpInfoMapBean) => null != pojo)
			.toDF()
		save(empInfoMapStreamDF, TableMapping.EMP_INFO_MAP)
		
		val expressBillStreamDF: DataFrame = oggBeanStreamDS
			.filter((bean: OggMessageBean) => bean.getTable == TableMapping.EXPRESS_BILL)
			.map((bean: OggMessageBean) => DataParser.toExpressBill(bean))
			.filter((pojo: ExpressBillBean) => null != pojo)
			.toDF()
		save(expressBillStreamDF, TableMapping.EXPRESS_BILL)
		
		val expressPackageStreamDF: DataFrame = oggBeanStreamDS
			.filter((bean: OggMessageBean) => bean.getTable == TableMapping.EXPRESS_PACKAGE)
			.map((bean: OggMessageBean) => DataParser.toExpressPackage(bean))
			.filter((pojo: ExpressPackageBean) => null != pojo)
			.toDF()
		save(expressPackageStreamDF, TableMapping.EXPRESS_PACKAGE)
		
		val fixedAreaStreamDF: DataFrame = oggBeanStreamDS
			.filter((bean: OggMessageBean) => bean.getTable == TableMapping.FIXED_AREA)
			.map((bean: OggMessageBean) => DataParser.toFixedArea(bean))
			.filter((pojo: FixedAreaBean) => null != pojo)
			.toDF()
		save(fixedAreaStreamDF, TableMapping.FIXED_AREA)
		
		val goodsRackStreamDF: DataFrame = oggBeanStreamDS
			.filter((bean: OggMessageBean) => bean.getTable == TableMapping.GOODS_RACK)
			.map((bean: OggMessageBean) => DataParser.toGoodsRack(bean))
			.filter((pojo: GoodsRackBean) => null != pojo)
			.toDF()
		save(goodsRackStreamDF, TableMapping.GOODS_RACK)
		
		val jobStreamDF: DataFrame = oggBeanStreamDS
			.filter((bean: OggMessageBean) => bean.getTable == TableMapping.JOB)
			.map((bean: OggMessageBean) => DataParser.toJob(bean))
			.filter((pojo: JobBean) => null != pojo)
			.toDF()
		save(jobStreamDF, TableMapping.JOB)
		
		val outWarehouseStreamDF: DataFrame = oggBeanStreamDS
			.filter((bean: OggMessageBean) => bean.getTable == TableMapping.OUT_WAREHOUSE)
			.map((bean: OggMessageBean) => DataParser.toOutWarehouse(bean))
			.filter((pojo: OutWarehouseBean) => null != pojo)
			.toDF()
		save(outWarehouseStreamDF, TableMapping.OUT_WAREHOUSE)
		
		val outWarehouseDetailStreamDF: DataFrame = oggBeanStreamDS
			.filter((bean: OggMessageBean) => bean.getTable == TableMapping.OUT_WAREHOUSE_DETAIL)
			.map((bean: OggMessageBean) => DataParser.toOutWarehouseDetail(bean))
			.filter((pojo: OutWarehouseDetailBean) => null != pojo)
			.toDF()
		save(outWarehouseDetailStreamDF, TableMapping.OUT_WAREHOUSE_DETAIL)
		
		val pkgStreamDF: DataFrame = oggBeanStreamDS
			.filter((bean: OggMessageBean) => bean.getTable == TableMapping.PKG)
			.map((bean: OggMessageBean) => DataParser.toPkg(bean))
			.filter((pojo: PkgBean) => null != pojo)
			.toDF()
		save(pkgStreamDF, TableMapping.PKG)
		
		val postalStandardStreamDF: DataFrame = oggBeanStreamDS
			.filter((bean: OggMessageBean) => bean.getTable == TableMapping.POSTAL_STANDARD)
			.map((bean: OggMessageBean) => DataParser.toPostalStandard(bean))
			.filter((pojo: PostalStandardBean) => null != pojo)
			.toDF()
		save(postalStandardStreamDF, TableMapping.POSTAL_STANDARD)
		
		val pushWarehouseStreamDF: DataFrame = oggBeanStreamDS
			.filter((bean: OggMessageBean) => bean.getTable == TableMapping.PUSH_WAREHOUSE)
			.map((bean: OggMessageBean) => DataParser.toPushWarehouse(bean))
			.filter((pojo: PushWarehouseBean) => null != pojo)
			.toDF()
		save(pushWarehouseStreamDF, TableMapping.PUSH_WAREHOUSE)
		
		val pushWarehouseDetailStreamDF: DataFrame = oggBeanStreamDS
			.filter((bean: OggMessageBean) => bean.getTable == TableMapping.PUSH_WAREHOUSE_DETAIL)
			.map((bean: OggMessageBean) => DataParser.toPushWarehouseDetail(bean))
			.filter((pojo: PushWarehouseDetailBean) => null != pojo)
			.toDF()
		save(pushWarehouseDetailStreamDF, TableMapping.PUSH_WAREHOUSE_DETAIL)
		
		val routeStreamDF: DataFrame = oggBeanStreamDS
			.filter((bean: OggMessageBean) => bean.getTable == TableMapping.ROUTE)
			.map((bean: OggMessageBean) => DataParser.toRoute(bean))
			.filter((pojo: RouteBean) => null != pojo)
			.toDF()
		save(routeStreamDF, TableMapping.ROUTE)
		
		val serviceEvaluationStreamDF: DataFrame = oggBeanStreamDS
			.filter((bean: OggMessageBean) => bean.getTable == TableMapping.SERVICE_EVALUATION)
			.map((bean: OggMessageBean) => DataParser.toServiceEvaluation(bean))
			.filter((pojo: ServiceEvaluationBean) => null != pojo)
			.toDF()
		save(serviceEvaluationStreamDF, TableMapping.SERVICE_EVALUATION)
		
		val storeGridStreamDF: DataFrame = oggBeanStreamDS
			.filter((bean: OggMessageBean) => bean.getTable == TableMapping.STORE_GRID)
			.map((bean: OggMessageBean) => DataParser.toStoreGrid(bean))
			.filter((pojo: StoreGridBean) => null != pojo)
			.toDF()
		save(storeGridStreamDF, TableMapping.STORE_GRID)
		
		val transportToolStreamDF: DataFrame = oggBeanStreamDS
			.filter((bean: OggMessageBean) => bean.getTable == TableMapping.TRANSPORT_TOOL)
			.map((bean: OggMessageBean) => DataParser.toTransportTool(bean))
			.filter((pojo: TransportToolBean) => null != pojo)
			.toDF()
		save(transportToolStreamDF, TableMapping.TRANSPORT_TOOL)
		
		val vehicleMonitorStreamDF: DataFrame = oggBeanStreamDS
			.filter((bean: OggMessageBean) => bean.getTable == TableMapping.VEHICLE_MONITOR)
			.map((bean: OggMessageBean) => DataParser.toVehicleMonitor(bean))
			.filter((pojo: VehicleMonitorBean) => null != pojo)
			.toDF()
		save(vehicleMonitorStreamDF, TableMapping.VEHICLE_MONITOR)
		
		val warehouseStreamDF: DataFrame = oggBeanStreamDS
			.filter((bean: OggMessageBean) => bean.getTable == TableMapping.WAREHOUSE)
			.map((bean: OggMessageBean) => DataParser.toWarehouse(bean))
			.filter((pojo: WarehouseBean) => null != pojo)
			.toDF()
		save(warehouseStreamDF, TableMapping.WAREHOUSE)
		
		val warehouseEmpStreamDF: DataFrame = oggBeanStreamDS
			.filter((bean: OggMessageBean) => bean.getTable == TableMapping.WAREHOUSE_EMP)
			.map((bean: OggMessageBean) => DataParser.toWarehouseEmp(bean))
			.filter((pojo: WarehouseEmpBean) => null != pojo)
			.toDF()
		save(warehouseEmpStreamDF, TableMapping.WAREHOUSE_EMP)
		
		val warehouseRackMapStreamDF: DataFrame = oggBeanStreamDS
			.filter((bean: OggMessageBean) => bean.getTable == TableMapping.WAREHOUSE_RACK_MAP)
			.map((bean: OggMessageBean) => DataParser.toWarehouseRackMap(bean))
			.filter((pojo: WarehouseRackMapBean) => null != pojo)
			.toDF()
		save(warehouseRackMapStreamDF, TableMapping.WAREHOUSE_RACK_MAP)
		
		val warehouseReceiptStreamDF: DataFrame = oggBeanStreamDS
			.filter((bean: OggMessageBean) => bean.getTable == TableMapping.WAREHOUSE_RECEIPT)
			.map((bean: OggMessageBean) => DataParser.toWarehouseReceipt(bean))
			.filter((pojo: WarehouseReceiptBean) => null != pojo)
			.toDF()
		save(warehouseReceiptStreamDF, TableMapping.WAREHOUSE_RECEIPT)
		
		val warehouseReceiptDetailStreamDF: DataFrame = oggBeanStreamDS
			.filter((bean: OggMessageBean) => bean.getTable == TableMapping.WAREHOUSE_RECEIPT_DETAIL)
			.map((bean: OggMessageBean) => DataParser.toWarehouseReceiptDetail(bean))
			.filter((pojo: WarehouseReceiptDetailBean) => null != pojo)
			.toDF()
		save(warehouseReceiptDetailStreamDF, TableMapping.WAREHOUSE_RECEIPT_DETAIL)
		
		val warehouseTransportToolStreamDF: DataFrame = oggBeanStreamDS
			.filter((bean: OggMessageBean) => bean.getTable == TableMapping.WAREHOUSE_TRANSPORT_TOOL)
			.map((bean: OggMessageBean) => DataParser.toWarehouseTransportTool(bean))
			.filter((pojo: WarehouseTransportToolBean) => null != pojo)
			.toDF()
		save(warehouseTransportToolStreamDF, TableMapping.WAREHOUSE_TRANSPORT_TOOL)
		
		val warehouseVehicleMapStreamDF: DataFrame = oggBeanStreamDS
			.filter((bean: OggMessageBean) => bean.getTable == TableMapping.WAREHOUSE_VEHICLE_MAP)
			.map((bean: OggMessageBean) => DataParser.toWarehouseVehicleMap(bean))
			.filter((pojo: WarehouseVehicleMapBean) => null != pojo)
			.toDF()
		save(warehouseVehicleMapStreamDF, TableMapping.WAREHOUSE_VEHICLE_MAP)
		
		val waybillStreamDF: DataFrame = oggBeanStreamDS
			.filter((bean: OggMessageBean) => bean.getTable == TableMapping.WAY_BILL)
			.map((bean: OggMessageBean) => DataParser.toWaybill(bean))
			.filter((pojo: WaybillBean) => null != pojo)
			.toDF()
		save(waybillStreamDF, TableMapping.WAY_BILL)
		
		val waybillStateRecordStreamDF: DataFrame = oggBeanStreamDS
			.filter((bean: OggMessageBean) => bean.getTable == TableMapping.WAYBILL_STATE_RECORD)
			.map((bean: OggMessageBean) => DataParser.toWaybillStateRecord(bean))
			.filter((pojo: WaybillStateRecordBean) => null != pojo)
			.toDF()
		save(waybillStateRecordStreamDF, TableMapping.WAYBILL_STATE_RECORD)
		
		val workTimeStreamDF: DataFrame = oggBeanStreamDS
			.filter((bean: OggMessageBean) => bean.getTable == TableMapping.WORK_TIME)
			.map((bean: OggMessageBean) => DataParser.toWorkTime(bean))
			.filter((pojo: WorkTimeBean) => null != pojo)
			.toDF()
		save(workTimeStreamDF, TableMapping.WORK_TIME)
		
		val transportRecordStreamDF: DataFrame = oggBeanStreamDS
			.filter((bean: OggMessageBean) => bean.getTable == TableMapping.TRANSPORT_RECORD)
			.map((bean: OggMessageBean) => DataParser.toTransportRecordBean(bean))
			.filter((pojo: TransportRecordBean) => null != pojo)
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
			.filter((bean: CanalMessageBean) => TableMapping.ADDRESS.equals(bean.getTable))
			// 提取字段值，封装到对应表POJO对象
			.flatMap((bean: CanalMessageBean) => DataParser.toAddressBean(bean))
			// 过滤null数据（解析转换失败）
			.filter((pojo: AddressBean) => null != pojo)
		save(pojoDS.toDF(), TableMapping.ADDRESS)
		
		// Customer 表数据
		val customerStreamDS: Dataset[CustomerBean] = canalBeanStreamDS
			.filter((bean: CanalMessageBean) => bean.getTable == TableMapping.CUSTOMER)
			.map((bean: CanalMessageBean) => DataParser.toCustomer(bean))
			.filter((pojo: CustomerBean) => null != pojo)
		save(customerStreamDS.toDF(), TableMapping.CUSTOMER)
		// ConsumerAddressMap 表数据
		val consumerAddressMapStreamDS: Dataset[ConsumerAddressMapBean] = canalBeanStreamDS
			.filter((bean: CanalMessageBean) => bean.getTable == TableMapping.CONSUMER_ADDRESS_MAP)
			.map((bean: CanalMessageBean) => DataParser.toConsumerAddressMap(bean))
			.filter((pojo: ConsumerAddressMapBean) => null != pojo)
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
			val conf: SparkConf = SparkUtils.autoSettingEnv(sparkConf)
			// c. 获取SparkSession对象
			SparkUtils.createSparkSession(conf, this.getClass)
		}
		import spark.implicits._
		
		// step2. 从Kafka数据源实时消费数据
		val logisticsStreamDF: DataFrame = load(spark, "logistics")
		//val crmStreamDF: DataFrame = load(spark, "crm")
		
		// step3. 对获取Json数据进行ETL转换(json -> bean)
		val logisticsEtlStreamDF: DataFrame = process(logisticsStreamDF, "logistics")
		//val crmEtlStreamDF: DataFrame = process(crmStreamDF, "crm")
		
		// step4. 保存转换后数据到外部存储(bean-> pojo和save）
		etlLogistics(logisticsEtlStreamDF)
		//etlCrm(crmEtlStreamDF)

		// step5. 应用启动以后，等待终止结束
		spark.streams.active.foreach((query: StreamingQuery) => println(s"Query: ${query.name} is Running ............"))
		spark.streams.awaitAnyTermination()
	}
}

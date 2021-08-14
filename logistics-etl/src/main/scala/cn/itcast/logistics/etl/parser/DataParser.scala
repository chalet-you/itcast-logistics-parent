package cn.itcast.logistics.etl.parser

import java.util
import java.util.Objects

import cn.itcast.logistics.common.beans.crm._
import cn.itcast.logistics.common.beans.logistics._
import cn.itcast.logistics.common.beans.parser._
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import org.apache.commons.collections.CollectionUtils

object DataParser {
	
	
	
	/**
	 * 判断messageBean是否是OggMessageBean
	 */
	private def getOggMessageBean(bean: MessageBean): OggMessageBean = {
		bean match {
			case ogg: OggMessageBean => ogg
		}
	}
	
	/**
	 * 判断messageBean是否是CanalMessageBean
	 */
	private def getCanalMessageBean(bean: MessageBean): CanalMessageBean = {
		bean match {
			case canal: CanalMessageBean => canal
		}
	}
	
	/**
	 * 提取ogg（I、U、D）和canal（insert、update、delete）数据的optype属性，转换成统一的操作字符串
	 *
	 * @param opType 数据操作类型：insert、update、delete，任意一种
	 */
	private def getOpType(opType: String): String = {
		opType match {
			case "I" => "insert"
			case "U" => "update"
			case "D" => "delete"
			case "INSERT" => "insert"
			case "UPDATE" => "update"
			case "DELETE" => "delete"
			case _ => "insert"
		}
	}
	
	// ================== 客户关系管理CRM系统业务数据解析 ==================
	// TODO: 将CRM系统业务数据：crm_address 表业务数据转换为POJO对象
	/*
		"data": [{
		"id": "10001",
		"name": "葛秋红",
		"tel": null,
		"mobile": "17*******47",
		"detail_addr": "恒大影城南侧小金庄",
		"area_id": "130903",
		"gis_addr": null,
		"cdt": "2020-02-02 18:51:39",
		"udt": "2020-02-02 18:51:39",
		"remark": null
		}]
	*/
	def toAddressBean(bean: MessageBean): Array[AddressBean] = {
		// a. 转换CanalBean对象
		val canalBean: CanalMessageBean = getCanalMessageBean(bean)
		
		// b. 提取字段值
		val opType: String = getOpType(canalBean.getType)
		val dataList: util.List[util.Map[String, AnyRef]] = canalBean.getData
		
		// c. 转换dataList数据为具体对象
		// list -> json
		val jsonList: String = JSON.toJSONString(dataList, true)
		// json -> pojo
		val addressBeans: util.List[AddressBean] = JSON.parseArray(jsonList, classOf[AddressBean])
		
		var beans: Array[AddressBean] = Array.empty
		// d. 如果不为null，并且有数据，进行设置opType
		if(null != addressBeans && addressBeans.size() > 0){
			// 导入隐式转换，将Java列表转换为Scala中集合对象
			import scala.collection.JavaConverters._
			// 遍历列表，对每个bean对象设置操作类型
			beans = addressBeans.asScala.map{bean =>
				// 设置操作类型
				bean.setOpType(opType)
				// 返回对象
				bean
			}.toArray
		}
		
		// e. 返回转换对象
		beans
	}
	
	def toCustomer(bean: MessageBean): CustomerBean = {
		var res: CustomerBean = null
		val canal = getCanalMessageBean(bean)
		val list: java.util.List[CustomerBean] = JSON.parseArray(JSON.toJSONString(canal.getData, SerializerFeature.PrettyFormat), classOf[CustomerBean])
		if (!CollectionUtils.isEmpty(list)) {
			res = list.get(0)
			res.setOpType(getOpType(canal.getType))
		}
		res
	}
	
	def toConsumerAddressMap(bean: MessageBean): ConsumerAddressMapBean = {
		var res = new ConsumerAddressMapBean
		val canal = getCanalMessageBean(bean)
		val list: java.util.List[ConsumerAddressMapBean] = JSON.parseArray(JSON.toJSONString(canal.getData, SerializerFeature.PrettyFormat), classOf[ConsumerAddressMapBean])
		if (!CollectionUtils.isEmpty(list)) {
			res = list.get(0)
			res.setOpType(getOpType(canal.getType))
		}
		res
	}
	
	// ================== 物流Logistics系统业务数据解析 ==================
	/*
		从MessageBean提取数据字段值和数据操作类型，将其封装到具体POJO对象中
		TODO: 将物流Logistics系统：tbl_areas表的字段信息转换成AreaBean对象
	 */
	def toAreaBean(bean: MessageBean): AreasBean = {
		// a. 转换Bean对象为具体类实例
		val oggBean: OggMessageBean = getOggMessageBean(bean)
		// b. 获取opType字段值
		val opType: String = getOpType(oggBean.getOp_type)
		// c. 获取数据字段值
		val dataMap: util.Map[String, AnyRef] = oggBean.getValue
		// d. 将数据值Map集合，封装到Pojo对象中， TODO： FastJson库
		/*
			第一步、将Map集合转换为JSON字符串，第二步、将JSON字符串转换为POJO对象
		 */
		val dataJson: String = JSON.toJSONString(dataMap, true)
		println(dataJson)
		val areasBean: AreasBean = JSON.parseObject(dataJson, classOf[AreasBean])
		//  e. 设置opType值
		if(null != areasBean) {
			areasBean.setOpType(opType)
		}
		// f. 返回pojo对象
		areasBean
	}
	
	
	def toChargeStandard(bean: MessageBean): ChargeStandardBean = {
		val ogg = getOggMessageBean(bean)
		val res = JSON.parseObject(JSON.toJSONString(ogg.getValue, SerializerFeature.PrettyFormat), classOf[ChargeStandardBean])
		if (Objects.nonNull(res)) {
			res.setOpType(getOpType(ogg.getOp_type))
		}
		res
	}
	
	def toCodes(bean: MessageBean): CodesBean = {
		val ogg = getOggMessageBean(bean)
		val res = JSON.parseObject(JSON.toJSONString(ogg.getValue, SerializerFeature.PrettyFormat), classOf[CodesBean])
		if (Objects.nonNull(res)) {
			res.setOpType(getOpType(ogg.getOp_type))
		}
		res
	}
	
	def toCollectPackage(bean: MessageBean): CollectPackageBean = {
		val ogg = getOggMessageBean(bean)
		val res = JSON.parseObject(JSON.toJSONString(ogg.getValue, SerializerFeature.PrettyFormat), classOf[CollectPackageBean])
		if (Objects.nonNull(res)) {
			res.setOpType(getOpType(ogg.getOp_type))
		}
		res
	}
	
	def toCompany(bean: MessageBean): CompanyBean = {
		val ogg = getOggMessageBean(bean)
		val res = JSON.parseObject(JSON.toJSONString(ogg.getValue, SerializerFeature.PrettyFormat), classOf[CompanyBean])
		if (Objects.nonNull(res)) {
			res.setOpType(getOpType(ogg.getOp_type))
		}
		res
	}
	
	def toCompanyDotMap(bean: MessageBean): CompanyDotMapBean = {
		val ogg = getOggMessageBean(bean)
		val res = JSON.parseObject(JSON.toJSONString(ogg.getValue, SerializerFeature.PrettyFormat), classOf[CompanyDotMapBean])
		if (Objects.nonNull(res)) {
			res.setOpType(getOpType(ogg.getOp_type))
		}
		res
	}
	
	def toCompanyTransportRouteMa(bean: MessageBean): CompanyTransportRouteMaBean = {
		val ogg = getOggMessageBean(bean)
		val res = JSON.parseObject(JSON.toJSONString(ogg.getValue, SerializerFeature.PrettyFormat), classOf[CompanyTransportRouteMaBean])
		if (Objects.nonNull(res)) {
			res.setOpType(getOpType(ogg.getOp_type))
		}
		res
	}
	
	def toCompanyWarehouseMap(bean: MessageBean): CompanyWarehouseMapBean = {
		val ogg = getOggMessageBean(bean)
		val res = JSON.parseObject(JSON.toJSONString(ogg.getValue, SerializerFeature.PrettyFormat), classOf[CompanyWarehouseMapBean])
		if (Objects.nonNull(res)) {
			res.setOpType(getOpType(ogg.getOp_type))
		}
		res
	}
	
	def toConsumerSenderInfo(bean: MessageBean): ConsumerSenderInfoBean = {
		val ogg = getOggMessageBean(bean)
		val res = JSON.parseObject(JSON.toJSONString(ogg.getValue, SerializerFeature.PrettyFormat), classOf[ConsumerSenderInfoBean])
		if (Objects.nonNull(res)) {
			res.setOpType(getOpType(ogg.getOp_type))
		}
		res
	}
	
	def toCourier(bean: MessageBean): CourierBean = {
		val ogg = getOggMessageBean(bean)
		val res = JSON.parseObject(JSON.toJSONString(ogg.getValue, SerializerFeature.PrettyFormat), classOf[CourierBean])
		if (Objects.nonNull(res)) {
			res.setOpType(getOpType(ogg.getOp_type))
		}
		res
	}
	
	def toDeliverPackage(bean: MessageBean): DeliverPackageBean = {
		val ogg = getOggMessageBean(bean)
		val res = JSON.parseObject(JSON.toJSONString(ogg.getValue, SerializerFeature.PrettyFormat), classOf[DeliverPackageBean])
		if (Objects.nonNull(res)) {
			res.setOpType(getOpType(ogg.getOp_type))
		}
		res
	}
	
	def toDeliverRegion(bean: MessageBean): DeliverRegionBean = {
		val ogg = getOggMessageBean(bean)
		val res = JSON.parseObject(JSON.toJSONString(ogg.getValue, SerializerFeature.PrettyFormat), classOf[DeliverRegionBean])
		if (Objects.nonNull(res)) {
			res.setOpType(getOpType(ogg.getOp_type))
		}
		res
	}
	
	def toDeliveryRecord(bean: MessageBean): DeliveryRecordBean = {
		val ogg = getOggMessageBean(bean)
		val res = JSON.parseObject(JSON.toJSONString(ogg.getValue, SerializerFeature.PrettyFormat), classOf[DeliveryRecordBean])
		if (Objects.nonNull(res)) {
			res.setOpType(getOpType(ogg.getOp_type))
		}
		res
	}
	
	def toDepartment(bean: MessageBean): DepartmentBean = {
		val ogg = getOggMessageBean(bean)
		val res = JSON.parseObject(JSON.toJSONString(ogg.getValue, SerializerFeature.PrettyFormat), classOf[DepartmentBean])
		if (Objects.nonNull(res)) {
			res.setOpType(getOpType(ogg.getOp_type))
		}
		res
	}
	
	def toDot(bean: MessageBean): DotBean = {
		val ogg = getOggMessageBean(bean)
		val res = JSON.parseObject(JSON.toJSONString(ogg.getValue, SerializerFeature.PrettyFormat), classOf[DotBean])
		if (Objects.nonNull(res)) {
			res.setOpType(getOpType(ogg.getOp_type))
		}
		res
	}
	
	def toDotTransportTool(bean: MessageBean): DotTransportToolBean = {
		val ogg = getOggMessageBean(bean)
		val res = JSON.parseObject(JSON.toJSONString(ogg.getValue, SerializerFeature.PrettyFormat), classOf[DotTransportToolBean])
		if (Objects.nonNull(res)) {
			res.setOpType(getOpType(ogg.getOp_type))
		}
		res
	}
	
	def toDriver(bean: MessageBean): DriverBean = {
		val ogg = getOggMessageBean(bean)
		val res = JSON.parseObject(JSON.toJSONString(ogg.getValue, SerializerFeature.PrettyFormat), classOf[DriverBean])
		if (Objects.nonNull(res)) {
			res.setOpType(getOpType(ogg.getOp_type))
		}
		res
	}
	
	def toEmp(bean: MessageBean): EmpBean = {
		val ogg = getOggMessageBean(bean)
		val res = JSON.parseObject(JSON.toJSONString(ogg.getValue, SerializerFeature.PrettyFormat), classOf[EmpBean])
		if (Objects.nonNull(res)) {
			res.setOpType(getOpType(ogg.getOp_type))
		}
		res
	}
	
	def toEmpInfoMap(bean: MessageBean): EmpInfoMapBean = {
		val ogg = getOggMessageBean(bean)
		val res = JSON.parseObject(JSON.toJSONString(ogg.getValue, SerializerFeature.PrettyFormat), classOf[EmpInfoMapBean])
		if (Objects.nonNull(res)) {
			res.setOpType(getOpType(ogg.getOp_type))
		}
		res
	}
	
	def toExpressBill(bean: MessageBean): ExpressBillBean = {
		val ogg = getOggMessageBean(bean)
		val res = JSON.parseObject(JSON.toJSONString(ogg.getValue, SerializerFeature.PrettyFormat), classOf[ExpressBillBean])
		if (Objects.nonNull(res)) {
			res.setOpType(getOpType(ogg.getOp_type))
		}
		res
	}
	
	def toExpressPackage(bean: MessageBean): ExpressPackageBean = {
		val ogg = getOggMessageBean(bean)
		val res = JSON.parseObject(JSON.toJSONString(ogg.getValue, SerializerFeature.PrettyFormat), classOf[ExpressPackageBean])
		if (Objects.nonNull(res)) {
			res.setOpType(getOpType(ogg.getOp_type))
		}
		res
	}
	
	def toFixedArea(bean: MessageBean): FixedAreaBean = {
		val ogg = getOggMessageBean(bean)
		val res = JSON.parseObject(JSON.toJSONString(ogg.getValue, SerializerFeature.PrettyFormat), classOf[FixedAreaBean])
		if (Objects.nonNull(res)) {
			res.setOpType(getOpType(ogg.getOp_type))
		}
		res
	}
	
	def toGoodsRack(bean: MessageBean): GoodsRackBean = {
		val ogg = getOggMessageBean(bean)
		val res = JSON.parseObject(JSON.toJSONString(ogg.getValue, SerializerFeature.PrettyFormat), classOf[GoodsRackBean])
		if (Objects.nonNull(res)) {
			res.setOpType(getOpType(ogg.getOp_type))
		}
		res
	}
	
	def toJob(bean: MessageBean): JobBean = {
		val ogg = getOggMessageBean(bean)
		val res = JSON.parseObject(JSON.toJSONString(ogg.getValue, SerializerFeature.PrettyFormat), classOf[JobBean])
		if (Objects.nonNull(res)) {
			res.setOpType(getOpType(ogg.getOp_type))
		}
		res
	}
	
	def toOutWarehouse(bean: MessageBean): OutWarehouseBean = {
		val ogg = getOggMessageBean(bean)
		val res = JSON.parseObject(JSON.toJSONString(ogg.getValue, SerializerFeature.PrettyFormat), classOf[OutWarehouseBean])
		if (Objects.nonNull(res)) {
			res.setOpType(getOpType(ogg.getOp_type))
		}
		res
	}
	
	def toOutWarehouseDetail(bean: MessageBean): OutWarehouseDetailBean = {
		val ogg = getOggMessageBean(bean)
		val res = JSON.parseObject(JSON.toJSONString(ogg.getValue, SerializerFeature.PrettyFormat), classOf[OutWarehouseDetailBean])
		if (Objects.nonNull(res)) {
			res.setOpType(getOpType(ogg.getOp_type))
		}
		res
	}
	
	def toPkg(bean: MessageBean): PkgBean = {
		val ogg = getOggMessageBean(bean)
		val res = JSON.parseObject(JSON.toJSONString(ogg.getValue, SerializerFeature.PrettyFormat), classOf[PkgBean])
		if (Objects.nonNull(res)) {
			res.setOpType(getOpType(ogg.getOp_type))
		}
		res
	}
	
	def toPostalStandard(bean: MessageBean): PostalStandardBean = {
		val ogg = getOggMessageBean(bean)
		val res = JSON.parseObject(JSON.toJSONString(ogg.getValue, SerializerFeature.PrettyFormat), classOf[PostalStandardBean])
		if (Objects.nonNull(res)) {
			res.setOpType(getOpType(ogg.getOp_type))
		}
		res
	}
	
	def toPushWarehouse(bean: MessageBean): PushWarehouseBean = {
		val ogg = getOggMessageBean(bean)
		val res = JSON.parseObject(JSON.toJSONString(ogg.getValue, SerializerFeature.PrettyFormat), classOf[PushWarehouseBean])
		if (Objects.nonNull(res)) {
			res.setOpType(getOpType(ogg.getOp_type))
		}
		res
	}
	
	def toPushWarehouseDetail(bean: MessageBean): PushWarehouseDetailBean = {
		val ogg = getOggMessageBean(bean)
		val res = JSON.parseObject(JSON.toJSONString(ogg.getValue, SerializerFeature.PrettyFormat), classOf[PushWarehouseDetailBean])
		if (Objects.nonNull(res)) {
			res.setOpType(getOpType(ogg.getOp_type))
		}
		res
	}
	
	def toRoute(bean: MessageBean): RouteBean = {
		val ogg = getOggMessageBean(bean)
		val res = JSON.parseObject(JSON.toJSONString(ogg.getValue, SerializerFeature.PrettyFormat), classOf[RouteBean])
		if (Objects.nonNull(res)) {
			res.setOpType(getOpType(ogg.getOp_type))
		}
		res
	}
	
	def toServiceEvaluation(bean: MessageBean): ServiceEvaluationBean = {
		val ogg = getOggMessageBean(bean)
		val res = JSON.parseObject(JSON.toJSONString(ogg.getValue, SerializerFeature.PrettyFormat), classOf[ServiceEvaluationBean])
		if (Objects.nonNull(res)) {
			res.setOpType(getOpType(ogg.getOp_type))
		}
		res
	}
	
	def toStoreGrid(bean: MessageBean): StoreGridBean = {
		val ogg = getOggMessageBean(bean)
		val res = JSON.parseObject(JSON.toJSONString(ogg.getValue, SerializerFeature.PrettyFormat), classOf[StoreGridBean])
		if (Objects.nonNull(res)) {
			res.setOpType(getOpType(ogg.getOp_type))
		}
		res
	}
	
	def toTransportTool(bean: MessageBean): TransportToolBean = {
		val ogg = getOggMessageBean(bean)
		val res = JSON.parseObject(JSON.toJSONString(ogg.getValue, SerializerFeature.PrettyFormat), classOf[TransportToolBean])
		if (Objects.nonNull(res)) {
			res.setOpType(getOpType(ogg.getOp_type))
		}
		res
	}
	
	def toVehicleMonitor(bean: MessageBean): VehicleMonitorBean = {
		val ogg = getOggMessageBean(bean)
		val res = JSON.parseObject(JSON.toJSONString(ogg.getValue, SerializerFeature.PrettyFormat), classOf[VehicleMonitorBean])
		if (Objects.nonNull(res)) {
			res.setOpType(getOpType(ogg.getOp_type))
		}
		res
	}
	
	def toWarehouse(bean: MessageBean): WarehouseBean = {
		val ogg = getOggMessageBean(bean)
		val res = JSON.parseObject(JSON.toJSONString(ogg.getValue, SerializerFeature.PrettyFormat), classOf[WarehouseBean])
		if (Objects.nonNull(res)) {
			res.setOpType(getOpType(ogg.getOp_type))
		}
		res
	}
	
	def toWarehouseEmp(bean: MessageBean): WarehouseEmpBean = {
		val ogg = getOggMessageBean(bean)
		val res = JSON.parseObject(JSON.toJSONString(ogg.getValue, SerializerFeature.PrettyFormat), classOf[WarehouseEmpBean])
		if (Objects.nonNull(res)) {
			res.setOpType(getOpType(ogg.getOp_type))
		}
		res
	}
	
	def toWarehouseRackMap(bean: MessageBean): WarehouseRackMapBean = {
		val ogg = getOggMessageBean(bean)
		val res = JSON.parseObject(JSON.toJSONString(ogg.getValue, SerializerFeature.PrettyFormat), classOf[WarehouseRackMapBean])
		if (Objects.nonNull(res)) {
			res.setOpType(getOpType(ogg.getOp_type))
		}
		res
	}
	
	def toWarehouseReceipt(bean: MessageBean): WarehouseReceiptBean = {
		val ogg = getOggMessageBean(bean)
		val res = JSON.parseObject(JSON.toJSONString(ogg.getValue, SerializerFeature.PrettyFormat), classOf[WarehouseReceiptBean])
		if (Objects.nonNull(res)) {
			res.setOpType(getOpType(ogg.getOp_type))
		}
		res
	}
	
	def toWarehouseReceiptDetail(bean: MessageBean): WarehouseReceiptDetailBean = {
		val ogg = getOggMessageBean(bean)
		val res = JSON.parseObject(JSON.toJSONString(ogg.getValue, SerializerFeature.PrettyFormat), classOf[WarehouseReceiptDetailBean])
		if (Objects.nonNull(res)) {
			res.setOpType(getOpType(ogg.getOp_type))
		}
		res
	}
	
	def toWarehouseSendVehicle(bean: MessageBean): WarehouseSendVehicleBean = {
		val ogg = getOggMessageBean(bean)
		val res = JSON.parseObject(JSON.toJSONString(ogg.getValue, SerializerFeature.PrettyFormat), classOf[WarehouseSendVehicleBean])
		if (Objects.nonNull(res)) {
			res.setOpType(getOpType(ogg.getOp_type))
		}
		res
	}
	
	def toWarehouseTransportTool(bean: MessageBean): WarehouseTransportToolBean = {
		val ogg = getOggMessageBean(bean)
		val res = JSON.parseObject(JSON.toJSONString(ogg.getValue, SerializerFeature.PrettyFormat), classOf[WarehouseTransportToolBean])
		if (Objects.nonNull(res)) {
			res.setOpType(getOpType(ogg.getOp_type))
		}
		res
	}
	
	def toWarehouseVehicleMap(bean: MessageBean): WarehouseVehicleMapBean = {
		val ogg = getOggMessageBean(bean)
		val res = JSON.parseObject(JSON.toJSONString(ogg.getValue, SerializerFeature.PrettyFormat), classOf[WarehouseVehicleMapBean])
		if (Objects.nonNull(res)) {
			res.setOpType(getOpType(ogg.getOp_type))
		}
		res
	}
	
	def toWaybill(bean: MessageBean): WaybillBean = {
		val ogg = getOggMessageBean(bean)
		val res = JSON.parseObject(JSON.toJSONString(ogg.getValue, SerializerFeature.PrettyFormat), classOf[WaybillBean])
		if (Objects.nonNull(res)) {
			res.setOpType(getOpType(ogg.getOp_type))
		}
		res
	}
	
	def toWaybillLine(bean: MessageBean): WaybillLineBean = {
		val ogg = getOggMessageBean(bean)
		val res = JSON.parseObject(JSON.toJSONString(ogg.getValue, SerializerFeature.PrettyFormat), classOf[WaybillLineBean])
		if (Objects.nonNull(res)) {
			res.setOpType(getOpType(ogg.getOp_type))
		}
		res
	}
	
	def toWaybillStateRecord(bean: MessageBean): WaybillStateRecordBean = {
		val ogg = getOggMessageBean(bean)
		val res = JSON.parseObject(JSON.toJSONString(ogg.getValue, SerializerFeature.PrettyFormat), classOf[WaybillStateRecordBean])
		if (Objects.nonNull(res)) {
			res.setOpType(getOpType(ogg.getOp_type))
		}
		res
	}
	
	def toWorkTime(bean: MessageBean): WorkTimeBean = {
		val ogg = getOggMessageBean(bean)
		val res = JSON.parseObject(JSON.toJSONString(ogg.getValue, SerializerFeature.PrettyFormat), classOf[WorkTimeBean])
		if (Objects.nonNull(res)) {
			res.setOpType(getOpType(ogg.getOp_type))
		}
		res
	}
	
	def toTransportRecordBean(bean: MessageBean): TransportRecordBean = {
		val ogg = getOggMessageBean(bean)
		val res = JSON.parseObject(JSON.toJSONString(ogg.getValue, SerializerFeature.PrettyFormat), classOf[TransportRecordBean])
		if (Objects.nonNull(res)) {
			res.setOpType(getOpType(ogg.getOp_type))
		}
		res
	}
}

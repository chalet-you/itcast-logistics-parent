package cn.itcast.logistics.common

import cn.itcast.logistics.common.beans.crm._
import cn.itcast.logistics.common.beans.logistics._
import cn.itcast.logistics.common.beans.parser.{CanalMessageBean, OggMessageBean}
import org.apache.spark.sql.{Encoder, Encoders}

/**
 * 扩展自定义POJO的隐式转换实现
 */
object BeanImplicits {

  // 定义MessageBean隐式参数Encoder值
  implicit def newOggMessageBeanEncoder: Encoder[OggMessageBean] = Encoders.bean(classOf[OggMessageBean])

  implicit def newCanalMessageBeanEncoder: Encoder[CanalMessageBean] = Encoders.bean(classOf[CanalMessageBean])

  // Logistics Bean
  implicit def newAreasBeanEncoder: Encoder[AreasBean] = Encoders.bean(classOf[AreasBean])

  implicit def newChargeStandardBeanEncoder: Encoder[ChargeStandardBean] = Encoders.bean(classOf[ChargeStandardBean])

  implicit def newCodesBeanEncoder: Encoder[CodesBean] = Encoders.bean(classOf[CodesBean])

  implicit def newCollectPackageBeanEncoder: Encoder[CollectPackageBean] = Encoders.bean(classOf[CollectPackageBean])

  implicit def newCompanyBeanEncoder: Encoder[CompanyBean] = Encoders.bean(classOf[CompanyBean])

  implicit def newCompanyDotMapBeanEncoder: Encoder[CompanyDotMapBean] = Encoders.bean(classOf[CompanyDotMapBean])

  implicit def newCompanyTransportRouteMaBeanEncoder: Encoder[CompanyTransportRouteMaBean] = Encoders.bean(classOf[CompanyTransportRouteMaBean])

  implicit def newCompanyWarehouseMapBeanEncoder: Encoder[CompanyWarehouseMapBean] = Encoders.bean(classOf[CompanyWarehouseMapBean])

  implicit def newConsumerSenderInfoBeanEncoder: Encoder[ConsumerSenderInfoBean] = Encoders.bean(classOf[ConsumerSenderInfoBean])

  implicit def newCourierBeanEncoder: Encoder[CourierBean] = Encoders.bean(classOf[CourierBean])

  implicit def newDeliverPackageBeanEncoder: Encoder[DeliverPackageBean] = Encoders.bean(classOf[DeliverPackageBean])

  implicit def newDeliverRegionBeanEncoder: Encoder[DeliverRegionBean] = Encoders.bean(classOf[DeliverRegionBean])

  implicit def newDeliveryRecordBeanEncoder: Encoder[DeliveryRecordBean] = Encoders.bean(classOf[DeliveryRecordBean])

  implicit def newDepartmentBeanEncoder: Encoder[DepartmentBean] = Encoders.bean(classOf[DepartmentBean])

  implicit def newDotBeanEncoder: Encoder[DotBean] = Encoders.bean(classOf[DotBean])

  implicit def newDotTransportToolBeanEncoder: Encoder[DotTransportToolBean] = Encoders.bean(classOf[DotTransportToolBean])

  implicit def newDriverBeanEncoder: Encoder[DriverBean] = Encoders.bean(classOf[DriverBean])

  implicit def newEmpBeanEncoder: Encoder[EmpBean] = Encoders.bean(classOf[EmpBean])

  implicit def newEmpInfoMapBeanEncoder: Encoder[EmpInfoMapBean] = Encoders.bean(classOf[EmpInfoMapBean])

  implicit def newExpressBillBeanEncoder: Encoder[ExpressBillBean] = Encoders.bean(classOf[ExpressBillBean])

  implicit def newExpressPackageBeanEncoder: Encoder[ExpressPackageBean] = Encoders.bean(classOf[ExpressPackageBean])

  implicit def newFixedAreaBeanEncoder: Encoder[FixedAreaBean] = Encoders.bean(classOf[FixedAreaBean])

  implicit def newGoodsRackBeanEncoder: Encoder[GoodsRackBean] = Encoders.bean(classOf[GoodsRackBean])

  implicit def newJobBeanEncoder: Encoder[JobBean] = Encoders.bean(classOf[JobBean])

  implicit def newOutWarehouseBeanEncoder: Encoder[OutWarehouseBean] = Encoders.bean(classOf[OutWarehouseBean])

  implicit def newOutWarehouseDetailBeanEncoder: Encoder[OutWarehouseDetailBean] = Encoders.bean(classOf[OutWarehouseDetailBean])

  implicit def newPkgBeanEncoder: Encoder[PkgBean] = Encoders.bean(classOf[PkgBean])

  implicit def newPostalStandardBeanEncoder: Encoder[PostalStandardBean] = Encoders.bean(classOf[PostalStandardBean])

  implicit def newPushWarehouseBeanEncoder: Encoder[PushWarehouseBean] = Encoders.bean(classOf[PushWarehouseBean])

  implicit def newPushWarehouseDetailBeanEncoder: Encoder[PushWarehouseDetailBean] = Encoders.bean(classOf[PushWarehouseDetailBean])

  implicit def newRouteBeanEncoder: Encoder[RouteBean] = Encoders.bean(classOf[RouteBean])

  implicit def newServiceEvaluationBeanEncoder: Encoder[ServiceEvaluationBean] = Encoders.bean(classOf[ServiceEvaluationBean])

  implicit def newStoreGridBeanEncoder: Encoder[StoreGridBean] = Encoders.bean(classOf[StoreGridBean])

  implicit def newTransportToolBeanEncoder: Encoder[TransportToolBean] = Encoders.bean(classOf[TransportToolBean])

  implicit def newVehicleMonitorBeanEncoder: Encoder[VehicleMonitorBean] = Encoders.bean(classOf[VehicleMonitorBean])

  implicit def newWarehouseBeanEncoder: Encoder[WarehouseBean] = Encoders.bean(classOf[WarehouseBean])

  implicit def newWarehouseEmpBeanEncoder: Encoder[WarehouseEmpBean] = Encoders.bean(classOf[WarehouseEmpBean])

  implicit def newWarehouseRackMapBeanEncoder: Encoder[WarehouseRackMapBean] = Encoders.bean(classOf[WarehouseRackMapBean])

  implicit def newWarehouseReceiptBeanEncoder: Encoder[WarehouseReceiptBean] = Encoders.bean(classOf[WarehouseReceiptBean])

  implicit def newWarehouseReceiptDetailBeanEncoder: Encoder[WarehouseReceiptDetailBean] = Encoders.bean(classOf[WarehouseReceiptDetailBean])

  implicit def newWarehouseSendVehicleBeanEncoder: Encoder[WarehouseSendVehicleBean] = Encoders.bean(classOf[WarehouseSendVehicleBean])

  implicit def newWarehouseTransportToolBeanEncoder: Encoder[WarehouseTransportToolBean] = Encoders.bean(classOf[WarehouseTransportToolBean])

  implicit def newWarehouseVehicleMapBeanEncoder: Encoder[WarehouseVehicleMapBean] = Encoders.bean(classOf[WarehouseVehicleMapBean])

  implicit def newWaybillBeanEncoder: Encoder[WaybillBean] = Encoders.bean(classOf[WaybillBean])

  implicit def newWaybillLineBeanEncoder: Encoder[WaybillLineBean] = Encoders.bean(classOf[WaybillLineBean])

  implicit def newWaybillStateRecordBeanEncoder: Encoder[WaybillStateRecordBean] = Encoders.bean(classOf[WaybillStateRecordBean])

  implicit def newWorkTimeBeanEncoder: Encoder[WorkTimeBean] = Encoders.bean(classOf[WorkTimeBean])

  implicit def newTransportRecordBeanEncoder: Encoder[TransportRecordBean] = Encoders.bean(classOf[TransportRecordBean])

  // CRM Bean
  implicit def newCustomerBeanEncoder: Encoder[CustomerBean] = Encoders.bean(classOf[CustomerBean])

  implicit def newAddressBeanEncoder: Encoder[AddressBean] = Encoders.bean(classOf[AddressBean])

  implicit def newConsumerAddressMapBeanEncoder: Encoder[ConsumerAddressMapBean] = Encoders.bean(classOf[ConsumerAddressMapBean])

}

package cn.itcast.logistics.generate;

import cn.itcast.logistics.common.utils.KuduHelper;

import java.util.List;
import java.util.Objects;

/**
 * 清空Kudu表的数据
 */
public class MockKuduDataApp {

    // Kudu
    private static final KuduHelper kuduHelper = KuduHelper.builder()
            .withMaster("node2.itcast.cn:7051")
            .build();

    public static void main( String[] args) {
        boolean isClean = true;
        if (args.length == 1 && args[0].matches("(true|false)")) {
            isClean = Boolean.parseBoolean(args[0]);
        }
        // 清空表
        if(isClean) {
            // 删除Kudu表
            dropKuduTables();
        }
        kuduHelper.close();
    }

    /**
     * 删除kudu中所有的表
     */
    private static void dropKuduTables() {
        final List<String> tables = kuduHelper.listTable();
        if (Objects.nonNull(tables)) {
            System.out.println("==== 开始删除的Kudu所有表，现有表："+tables.size()+" ====");
            tables.forEach(kuduHelper::deleteTable);
            System.out.println("==== 完成删除的Kudu所有表，现有表："+kuduHelper.listTable().size()+" ====");
        }
    }

    /**
     * ## 检查是否删除表或清空数据成功 ##
     * SQL1：在Hue中检查kudu表是否被全部删除
     *          使用方式：
     *              A 在浏览器中打开：http://node2.itcast.cn:8889/hue
     *              B 输入命令show tables或者复制SQL1粘贴到hue编辑框中执行，结果为0表示正确
     */
    private static void checkSQL() {
        String useHueCheckSQL = "SELECT SUM(t.cnt) FROM (\n" +
            "\t(SELECT COUNT(1) AS cnt FROM logistics.tbl_address) UNION \n" +
            "\t(SELECT COUNT(1) AS cnt FROM logistics.tbl_areas) UNION \n" +
            "\t(SELECT COUNT(1) AS cnt FROM logistics.tbl_charge_standard) UNION \n" +
            "\t(SELECT COUNT(1) AS cnt FROM logistics.tbl_codes) UNION \n" +
            "\t(SELECT COUNT(1) AS cnt FROM logistics.tbl_collect_package) UNION \n" +
            "\t(SELECT COUNT(1) AS cnt FROM logistics.tbl_company) UNION \n" +
            "\t(SELECT COUNT(1) AS cnt FROM logistics.tbl_company_dot_map) UNION \n" +
            "\t(SELECT COUNT(1) AS cnt FROM logistics.tbl_company_transport_route_ma) UNION \n" +
            "\t(SELECT COUNT(1) AS cnt FROM logistics.tbl_company_warehouse_map) UNION \n" +
            "\t(SELECT COUNT(1) AS cnt FROM logistics.tbl_consumer_address_map) UNION \n" +
            "\t(SELECT COUNT(1) AS cnt FROM logistics.tbl_consumer_sender_info) UNION \n" +
            "\t(SELECT COUNT(1) AS cnt FROM logistics.tbl_courier) UNION \n" +
            "\t(SELECT COUNT(1) AS cnt FROM logistics.tbl_customer) UNION \n" +
            "\t(SELECT COUNT(1) AS cnt FROM logistics.tbl_deliver_package) UNION \n" +
            "\t(SELECT COUNT(1) AS cnt FROM logistics.tbl_deliver_region) UNION \n" +
            "\t(SELECT COUNT(1) AS cnt FROM logistics.tbl_delivery_record) UNION \n" +
            "\t(SELECT COUNT(1) AS cnt FROM logistics.tbl_department) UNION \n" +
            "\t(SELECT COUNT(1) AS cnt FROM logistics.tbl_dot) UNION \n" +
            "\t(SELECT COUNT(1) AS cnt FROM logistics.tbl_dot_transport_tool) UNION \n" +
            "\t(SELECT COUNT(1) AS cnt FROM logistics.tbl_driver) UNION \n" +
            "\t(SELECT COUNT(1) AS cnt FROM logistics.tbl_emp) UNION \n" +
            "\t(SELECT COUNT(1) AS cnt FROM logistics.tbl_emp_info_map) UNION \n" +
            "\t(SELECT COUNT(1) AS cnt FROM logistics.tbl_express_bill) UNION \n" +
            "\t(SELECT COUNT(1) AS cnt FROM logistics.tbl_express_package) UNION \n" +
            "\t(SELECT COUNT(1) AS cnt FROM logistics.tbl_fixed_area) UNION \n" +
            "\t(SELECT COUNT(1) AS cnt FROM logistics.tbl_goods_rack) UNION \n" +
            "\t(SELECT COUNT(1) AS cnt FROM logistics.tbl_job) UNION \n" +
            "\t(SELECT COUNT(1) AS cnt FROM logistics.tbl_out_warehouse) UNION \n" +
            "\t(SELECT COUNT(1) AS cnt FROM logistics.tbl_pkg) UNION \n" +
            "\t(SELECT COUNT(1) AS cnt FROM logistics.tbl_postal_standard) UNION \n" +
            "\t(SELECT COUNT(1) AS cnt FROM logistics.tbl_push_warehouse) UNION \n" +
            "\t(SELECT COUNT(1) AS cnt FROM logistics.tbl_route) UNION \n" +
            "\t(SELECT COUNT(1) AS cnt FROM logistics.tbl_service_evaluation) UNION \n" +
            "\t(SELECT COUNT(1) AS cnt FROM logistics.tbl_store_grid) UNION \n" +
            "\t(SELECT COUNT(1) AS cnt FROM logistics.tbl_transport_tool) UNION \n" +
            "\t(SELECT COUNT(1) AS cnt FROM logistics.tbl_vehicle_monitor) UNION \n" +
            "\t(SELECT COUNT(1) AS cnt FROM logistics.tbl_warehouse) UNION \n" +
            "\t(SELECT COUNT(1) AS cnt FROM logistics.tbl_warehouse_emp) UNION \n" +
            "\t(SELECT COUNT(1) AS cnt FROM logistics.tbl_warehouse_rack_map) UNION \n" +
            "\t(SELECT COUNT(1) AS cnt FROM logistics.tbl_warehouse_receipt) UNION \n" +
            "\t(SELECT COUNT(1) AS cnt FROM logistics.tbl_warehouse_send_vehicle) UNION \n" +
            "\t(SELECT COUNT(1) AS cnt FROM logistics.tbl_warehouse_transport_tool) UNION \n" +
            "\t(SELECT COUNT(1) AS cnt FROM logistics.tbl_warehouse_vehicle_map) UNION \n" +
            "\t(SELECT COUNT(1) AS cnt FROM logistics.tbl_waybill) UNION \n" +
            "\t(SELECT COUNT(1) AS cnt FROM logistics.tbl_waybill_line) UNION \n" +
            "\t(SELECT COUNT(1) AS cnt FROM logistics.tbl_waybill_state_record) UNION \n" +
            "\t(SELECT COUNT(1) AS cnt FROM logistics.tbl_work_time)\n" +
            ") AS t";
        System.out.println("==== SQL1(Using Hue Query): \n"+useHueCheckSQL);
    }

}

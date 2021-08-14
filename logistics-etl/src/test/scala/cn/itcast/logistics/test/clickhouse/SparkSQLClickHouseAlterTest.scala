package cn.itcast.logistics.test.clickhouse

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * 测试自定义数据源ClickHouse：
 *      将数据集批量保存至ClickHouse表中
 */
object SparkSQLClickHouseAlterTest {
	
	def main(args: Array[String]): Unit = {
		// 1. 构建SparkSession实例对象，设置相关配置信息
		val spark: SparkSession = SparkSession.builder()
			.appName(this.getClass.getSimpleName.stripSuffix("$"))
			.master("local[2]")
			.config("spark.sql.shuffle.partitions", "2")
			// 设置Kryo序列化方式
			.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
			.getOrCreate()
		import spark.implicits._
		
		// 2. 模拟产生数据
		val dataframe: DataFrame = Seq(
			("书籍", 100, "update"), ("家具", 100, "update"), ("食品", 100, "delete")
		).toDF("category", "total", "opType")
		
		// 3. 保存分析结果数据至ClickHouse表中
		dataframe.write
			.mode(SaveMode.Append)
			.format("clickhouse")
			.option("clickhouse.driver", "ru.yandex.clickhouse.ClickHouseDriver")
			.option("clickhouse.url", "jdbc:clickhouse://node2.itcast.cn:8123/")
			.option("clickhouse.user", "root")
			.option("clickhouse.password", "123456")
			.option("clickhouse.table", "test.tbl_order_agg")
			.option("clickhouse.auto.create", "true")  // 表不存在，创建表
			.option("clickhouse.primary.key", "category") // 指定主键字段名称
			.option("clickhouse.operate.field", "opType") // 指定数据操作类型的字段
			.save()
		
		// 应用结束，关闭资源
		spark.stop()
	}
	
}

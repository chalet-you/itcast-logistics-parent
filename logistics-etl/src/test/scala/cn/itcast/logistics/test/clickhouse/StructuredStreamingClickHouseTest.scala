package cn.itcast.logistics.test.clickhouse

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}

/**
 * 从TCP Socket读取数据，进行词频统计，将最终结果存储至ClickHouse表中
 */
object StructuredStreamingClickHouseTest {
	
	def main(args: Array[String]): Unit = {
		// 1. 构建SparkSession实例对象
		val spark: SparkSession = SparkSession.builder()
			.appName(this.getClass.getSimpleName.stripSuffix("$"))
			.master("local[3]")
			.config("spark.sql.shuffle.partitions", "3")
			.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
			.getOrCreate()
		import spark.implicits._
		
		// 2. 从TCP Socket消费数据，端口号为：9998
		val inputStreamDF: DataFrame = spark.readStream
			.format("socket")
			.option("host", "node2.itcast.cn")
			.option("port", "9998")
			.load()
		
		// 3. 进行词频统计，基于SQL函数实现（DSL编程）
		val resultStreamDF: DataFrame = inputStreamDF
			// 简易过滤数据
    		.where($"value".isNotNull && length(trim($"value")).gt(0))
			// 分割单词和行转列
    		.select(
			    explode(split(trim($"value"), "\\s+")).as("word")
		    )
			// 按照单词分组和聚合
    		.groupBy($"word").count()
		
		// 4. 保存流式数据到ClickHouse表中
		/*
			tableName: test.tbl_wordcount
			primaryKey: word
			column: count
		 */
		val query: StreamingQuery = resultStreamDF
    		.withColumn("opType", lit("insert"))
    		.writeStream
			.outputMode(OutputMode.Update())
			.queryName("query-clickhouse-tbl_word_count")
			.format("clickhouse")
			.option("clickhouse.driver", "ru.yandex.clickhouse.ClickHouseDriver")
			.option("clickhouse.url", "jdbc:clickhouse://node2.itcast.cn:8123/")
			.option("clickhouse.user", "root")
			.option("clickhouse.password", "123456")
			.option("clickhouse.table", "test.tbl_word_count")
			.option("clickhouse.auto.create", "true")
			.option("clickhouse.primary.key", "word")
			.option("clickhouse.operate.field", "opType")
			.option("checkpointLocation", "datas/ckpt-1001/")
			.start()
		
		// 5. 流式应用启动以后，等待终止结束
		println(s"Query: ${query.name} is Running .......................")
		spark.streams.awaitAnyTermination()
	}
	
}

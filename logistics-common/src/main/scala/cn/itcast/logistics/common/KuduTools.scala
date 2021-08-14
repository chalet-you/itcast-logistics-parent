package cn.itcast.logistics.common

import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * 操作Kudu数据库时工具类，比如创建Kudu表方法
 */
object KuduTools {
	
	/**
	 * 创建Kudu表，先判断表是否存在，不存在时，再创建，实现步骤：
		 * step1. 获取Kudu的上下文对象：KuduContext
		 * step2. 判断Kudu中是否存在这张表，如果不存在则创建
		 * step3. 生成Kudu表的结构信息
		 * step4. 设置表的分区策略和副本数目
		 * step5. 创建表
	 */
	def createKuduTable(tableName: String, dataframe: DataFrame, keys: Seq[String] = Seq("id")): Unit = {
		// step1. 获取Kudu的上下文对象：KuduContext
		val kuduContext: KuduContext = new KuduContext(Configuration.KUDU_RPC_ADDRESS, dataframe.sparkSession.sparkContext)
		
		// step2. 判断Kudu中是否存在这张表，如果不存在则创建
		if(kuduContext.tableExists(tableName)){
			println(s"Kudu Table: ${tableName} already exists ........................")
			return ;
		}
		
		// step3. 生成Kudu表的结构信息， TODO： Kudu表中主键列不可以为null，必须创建表时，显示指定
		val schema: StructType = StructType(
			dataframe.schema.map{field =>
				StructField(
					field.name,
					field.dataType,
					nullable = if(keys.contains(field.name)) false else true
				)
			}
		)
		
		// step4. 设置表的分区策略和副本数目
		val options: CreateTableOptions = new CreateTableOptions()
		// 设置副本数
		options.setNumReplicas(1)
		// 设置分区策略
		import scala.collection.JavaConverters._
		options.addHashPartitions(keys.asJava, 3)
		
		// step5. 创建表
		/*
		  def createTable(
		      tableName: String,
		      schema: StructType,
		      keys: Seq[String],
		      options: CreateTableOptions
		  ): KuduTable
		 */
		val kuduTable = kuduContext.createTable( tableName, schema, keys,  options )
		println(s"Kudu Table: ${tableName}-${kuduTable.getTableId} is created .....................")
	}
	
}

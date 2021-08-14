package cn.itcast.logistics.test

import com.alibaba.fastjson.JSON

object FastJsonTest {
	
	
	def main(args: Array[String]): Unit = {
		
		// 定义json字符串
		val jsonStr: String =
			"""
			  |{
			  |  "id": 10001,
			  |  "name": "zhangsan"
			  |}
			  |""".stripMargin
		// JSON -> JavaBean
		val stuBean: StuBean = JSON.parseObject(jsonStr, classOf[StuBean])
		println(stuBean)
		
		
		// JavaBean转换为JSON字符串
		val stuJson: String = JSON.toJSONString(stuBean, true)
		println(stuJson)
	}
	
}
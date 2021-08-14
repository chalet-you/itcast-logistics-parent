package cn.itcast.logistics.test;

import java.io.InputStream;
import java.util.Properties;
import java.util.ResourceBundle;

public class LoadPropertiesTest {

	public static void main(String[] args) throws Exception {
		// 获取属性文件流：InputStream
		InputStream inputStream = LoadPropertiesTest.class.getClassLoader()
				.getResourceAsStream("config.propoerties");
		// 将InputStream数据流加载为Properties实例对象值
		Properties properties = new Properties() ;
		properties.load(inputStream);


		// ====================================================================

		ResourceBundle bundle = ResourceBundle.getBundle("config");

	}

}

package cn.itcast.logistics.common.beans;

import java.io.Serializable;

/**
 * 数据库中业务数据对用Bean对象基类，公共字段OpType操作类型
 */
public class AbstractBean implements Serializable {

	private String opType = "insert";

	/**
	 * 1:insert、2:update、3:delete
	 */
	public String getOpType() {
		return opType;
	}

	public void setOpType(String opType) {
		this.opType = opType;
	}
}

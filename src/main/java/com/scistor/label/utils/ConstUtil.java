package com.scistor.label.utils;

/**
 * 常量工具
 * @author qiaobin
 */
public interface ConstUtil {

	//HBASE 最大版本数
	public final int MAX_VERSION = 100;

	//ES 单个type 阈值
	public final long ES_TYPE_EXCEED_NUM = 10000000;

	//ES type连接符
	public final String ES_TYPE_REGEX = "-";

	public enum DATABASE {
		HIVE, HBASE, ES
	}

}

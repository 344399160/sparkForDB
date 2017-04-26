package label.utils;

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

	public final String THRIFT_REGEX = "thrift://";

	public final String HIVE_SITE = "/hive-site.xml";

	public final String ROWKEY = "rowKey";

	public final String VERSION = "version";

	public final String COLUMNFAMILY_DEFAULT = "default";

	public enum DATABASE {
		HIVE, HBASE, ES
	}

	public final String HIVE_SITE_XML_CONTENT =
			"<?xml version=\"1.0\" encoding=\"UTF-8\"?> \n" +
					"<configuration> \n" +
					"\t<property> \n" +
					"\t\t<name>datanucleus.readOnlyDatastore</name> \n" +
					"\t\t<value>false</value> \n" +
					"\t</property> \n" +
					"\t<property> \n" +
					"\t\t<name>datanucleus.fixedDatastore</name> \n" +
					"\t\t<value>false</value> \n" +
					"\t</property> \n" +
					"\t<property> \n" +
					"\t\t<name>datanucleus.autoCreateSchema</name> \n" +
					"\t\t<value>true</value> \n" +
					"\t</property> \n" +
					"\t<property> \n" +
					"\t\t<name>datanucleus.autoCreateTables</name> \n" +
					"\t\t<value>true</value> \n" +
					"\t</property> \n" +
					"\t<property> \n" +
					"\t\t<name>datanucleus.autoCreateColumns</name> \n" +
					"\t\t<value>true</value> \n" +
					"\t</property> \n" +
					"\t<property> \n" +
					"\t\t<name>javax.jdo.option.ConnectionDriverName</name> \n" +
					"\t\t<value>com.mysql.jdbc.Driver</value> \n" +
					"<description>Driver class name for a JDBC metastore</description> \n" +
					"\t</property> \n" +
					"\t<property> \n" +
					"\t\t<name>hive.metastore.uris</name> \n" +
					"\t\t<value>{0}</value> \n" +
					"\t</property> \n" +
					"</configuration>";

}

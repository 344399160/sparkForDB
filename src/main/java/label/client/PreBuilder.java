package label.client;

import label.common.MessageException;
import label.model.AbstractConf;
import label.model.ESConf;
import label.model.HBaseConf;
import label.model.HiveConf;
import label.utils.ConstUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.FileWriter;
import java.text.MessageFormat;

/**
 * 描述：配置构造
 * author qiaobin   2017/4/18 16:58.
 */
public class PreBuilder {
    protected JavaSparkContext jsc;
    protected SparkConf sparkConf;
    protected HBaseConf hbaseOutConf;  //输出库-HBASE
    protected ESConf esOutConf;  //输出库-ES
    protected AbstractConf abstractConf;   //输入库：HIVE or HBASE
    protected String inputType;
//    protected String hiveUrl;

    public PreBuilder() {
        sparkConf  = new SparkConf().setAppName("StoreClient");
        //hbase配置
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer");
        //hive配置
        sparkConf.set("hive.metastore.schema.verification", "false");
    }

    public PreBuilder setMaster(String master) {
        sparkConf.setMaster(master);
        return this;
    }

    public PreBuilder setMemorySize(String memorySize) {
        sparkConf.set("spark.executor.memory", memorySize);
        return this;
    }

    /**
     * 功能描述：设置输入表
     * @author qiaobin
     * @date 2017/4/18  16:32
     * @param inputDBConf 通用输入配置， 支持 hbase、hive
     */
    public PreBuilder setInputSource(AbstractConf inputDBConf) {
        if (inputDBConf instanceof HBaseConf) {
            inputType = ConstUtil.DATABASE.HBASE.toString();
        }
        if (inputDBConf instanceof HiveConf) {
            inputType = ConstUtil.DATABASE.HIVE.toString();
            //TODO
//            String url = ((HiveConf) inputDBConf).getPath();
//            String formatUrl = formatHiveUrl(url);
//            if (!StringUtils.isNotEmpty(hiveUrl) || !formatUrl.equals(hiveUrl)) {
//                this.makeHiveSiteXml(formatUrl);
//            }
//            this.hiveUrl = formatUrl;
        }
        this.abstractConf = inputDBConf;
        return this;
    }

    /**
     * 功能描述：设置hbase输出表
     * @author qiaobin
     * @date 2017/4/18  16:33
     * @param hbaseOutConf hbase连接配置
     */
    public PreBuilder setHBaseOutputSource(HBaseConf hbaseOutConf) {
        this.hbaseOutConf = hbaseOutConf;
        return this;
    }

    /**
     * 功能描述：设置ES输出表
     * @author qiaobin
     * @date 2017/4/18  16:33
     * @param esOutConf es连接配置
     */
    public PreBuilder setESOutputSource(ESConf esOutConf) {
        this.esOutConf = esOutConf;
        sparkConf.set("es.nodes", esOutConf.getNodes());
        sparkConf.set("es.index.auto.create", "true");
        sparkConf.set("es.port", esOutConf.getHadoopPort());
        return this;
    }

    /**
     * 功能描述：创建JavaSparkContext连接
     * @author qiaobin
     * @date 2017/4/18  16:44
     * @param
     */
    public StoreClient client() {
        this.jsc = new JavaSparkContext(sparkConf);
        return new StoreClient(this);
    }

    //将 thrift://ip:9083/db/table 或 ip;ip;ip;thrift://ip:9083/db/table 提取成 thrift://ip:9083
    private String formatHiveUrl(String hiveUrl) {
        String regex = ConstUtil.THRIFT_REGEX;
        String url = "";
        if (!hiveUrl.startsWith(regex)) {
            hiveUrl =  hiveUrl.substring(hiveUrl.indexOf(regex), hiveUrl.length()-1);
        }
        url = hiveUrl.replace(regex, "");
        return regex + url.substring(0, url.indexOf("/"));
    }

    //生成hive-site.xml在引用工程根目录下
    private void makeHiveSiteXml(String hiveUrl) {
        try {
            //根目录路径
            String path = this.getClass().getClassLoader().getResource("").getPath();
            File output = new File(path + ConstUtil.HIVE_SITE);
            if (!output.exists()) {
                output.mkdir();
            }
            FileWriter fw = new FileWriter(output);
            fw.write(MessageFormat.format(ConstUtil.HIVE_SITE_XML_CONTENT, hiveUrl));
            fw.flush();
            fw.close();
        } catch (Exception e) {
            throw new MessageException(e);
        }
    }
}

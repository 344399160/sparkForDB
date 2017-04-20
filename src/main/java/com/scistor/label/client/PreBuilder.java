package com.scistor.label.client;

import com.scistor.label.model.AbstractConf;
import com.scistor.label.model.ESConf;
import com.scistor.label.model.HBaseConf;
import com.scistor.label.model.HiveConf;
import com.scistor.label.utils.ConstUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

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
}

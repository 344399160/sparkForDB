package com.scistor.label.client;

import com.scistor.label.common.*;
import com.scistor.label.elasticsearch.ESService;
import com.scistor.label.hbase.HBaseInputTable;
import com.scistor.label.hbase.HBaseOutputTable;
import com.scistor.label.hive.HCatInputTable;
import com.scistor.label.model.AbstractConf;
import com.scistor.label.model.ESConf;
import com.scistor.label.model.HBaseConf;
import com.scistor.label.model.HiveConf;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.spark.SerializableWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.text.MessageFormat;
import java.util.Map;

import static com.scistor.label.utils.ConstUtil.DATABASE;
/**
 * 描述：上层调用方法
 * author qiaobin   2017/4/14 09:54.
 */
public class StoreClient {

    private JavaSparkContext jsc;
    private HBaseConf hbaseOutConf;  //输出库-HBASE
    private ESConf esOutConf;  //输出库-ES
    private AbstractConf abstractConf;   //输入库：HIVE or HBASE
    private String inputType;

    protected StoreClient(PreBuilder constructor) {
        this.jsc = constructor.jsc;
        this.hbaseOutConf = constructor.hbaseOutConf;
        this.esOutConf = constructor.esOutConf;
        this.inputType = constructor.inputType;
        this.abstractConf = constructor.abstractConf;
    }

    /**
     * 功能描述：从数据源读取数据
     * @author qiaobin
     */
    public JavaPairRDD<String, Map<String, String>> readRDD() throws Exception{
        JavaPairRDD<String, Map<String, String>> pairRDD = null;
        if (inputType.equals(DATABASE.HIVE.toString())) {  //从hive表中获取数据
            HiveConf hiveConf = (HiveConf) abstractConf;
            hiveConf.check(); //检查必填值是否有空值
            HCatInputTable hcatInputTable = new HCatInputTable(hiveConf.getPath());  //"thrift://192.168.40.128:9083/default/test"
            //根据配置信息从hive表中取出相应字段值
            JavaPairRDD<WritableComparable, SerializableWritable<HCatRecord>> genresRdd = hcatInputTable.read(jsc, hiveConf.getColumns());
            //转为通用格式数据
            pairRDD = genresRdd.mapToPair(new HCatToMap(hcatInputTable.getSchema()));
        } else if (inputType.equals(DATABASE.HBASE.toString())) {  //从hbase表中获取数据
            HBaseConf hBaseConf = (HBaseConf) abstractConf;
            hBaseConf.check(); //检查必填值是否有空值
            HBaseInputTable inputTable = new HBaseInputTable(hBaseConf.getPath(), hBaseConf.getColumnFamily(), hBaseConf.getTable());
            JavaPairRDD<ImmutableBytesWritable, Result> rateRdd = inputTable.readSelectColumn(jsc, hBaseConf.getColumns());
            pairRDD = rateRdd.mapToPair(new ResultToMap());
        }
        return pairRDD;
    }

    /**
     * 功能描述：数据写入
     * @author qiaobin
     * @param pairRDD
     */
    public void write(JavaPairRDD<String, Map<String, String>> pairRDD, long timestamp, String rowKeyColumn) throws Exception{
        hbaseOutConf.check();
        JavaPairRDD<ImmutableBytesWritable, Put> putPairRDD;
        JavaPairRDD<String, Map<String, String>> esPairRDD;
        //如果输入库是hive则必须指定rowkey列
        if (inputType.equals(DATABASE.HIVE.toString()))
            if (!StringUtils.isNotEmpty(rowKeyColumn))
                throw new MessageException("rowKeyColumn can not be null;");

        //保存至hbase
        HBaseOutputTable outputTable = new HBaseOutputTable(hbaseOutConf.getPath(), hbaseOutConf.getColumnFamily(), hbaseOutConf.getTable());
        //rowKeyColumn 如果为空默认以输入库的主键或rowKey作为输出表的rowKey， 否则用指定字段作为rowkey
        if (StringUtils.isNotEmpty(rowKeyColumn)) {
            putPairRDD = pairRDD.mapToPair(new MapToPutWithRowKeyColumn(hbaseOutConf.getColumnFamily(), timestamp, rowKeyColumn));
        } else {
            putPairRDD = pairRDD.mapToPair(new MapToPut(hbaseOutConf.getColumnFamily(), timestamp));
        }
        outputTable.writeFromHbase(putPairRDD);

        //保存至ES
        if (null != esOutConf && esOutConf.check()) {
            //1. 检查 ES 索引是否存在, 不存在新建索引
            ESService service = new ESService(esOutConf.getClusterName(), esOutConf.getNodes(), Integer.parseInt(esOutConf.getClientPort()));
            if (!service.indexExist(esOutConf.getIndex())) {
                service.createIndex(esOutConf.getIndex());
            }
            //2. 自动生成type， 此处需要校验最后一个type数据是否超过阈值. 超出则新建type, 没超出则用最后一个type
            String type = service.generateType(esOutConf.getIndex(), timestamp);
            //3. 将rowKey作为hbase的索引字段, timestamp 作为版本字段插入库中,
            if (StringUtils.isNotEmpty(rowKeyColumn)) {
                esPairRDD = pairRDD.mapToPair(new MapToESMapWithRowKeyColumn(timestamp, rowKeyColumn));
            } else {
                esPairRDD = pairRDD.mapToPair(new MapToESMap(timestamp));
            }
            JavaEsSpark.saveToEsWithMeta(esPairRDD, MessageFormat.format("{0}/{1}", esOutConf.getIndex(), type));
        }
    }

    /**
      * 功能描述：关闭
      * @author qiaobin
      */
    public void close() {
        this.jsc.close();
    }

}



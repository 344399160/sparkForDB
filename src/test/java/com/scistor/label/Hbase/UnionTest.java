package com.scistor.label.Hbase;

import com.scistor.label.client.StoreClient;
import com.scistor.label.client.PreBuilder;
import com.scistor.label.model.ESConf;
import com.scistor.label.model.HBaseConf;
import com.scistor.label.model.HiveConf;
import com.scistor.label.common.VersionGenerator;
import org.apache.spark.api.java.JavaPairRDD;
import org.junit.Test;

import java.util.Map;

/**
 * 描述：TODO
 * author qiaobin   2017/4/14 14:19.
 */
public class UnionTest {

    @Test
    public void test() {
        // hbase 作为输入源
//        HBaseConf inputConf = new HBaseConf()
//                .setPath("172.16.9.41,172.16.9.42,172.16.9.43;2181;hdfs://172.16.9.41:8020/hbase")
//                .setTable("ctt_test")
//                .setColumnFamily("default")
//                .setColumns(new String[]{"default:Tag"});
        // hive 作为输入源
        HiveConf inputConf = new HiveConf()
                .setPath("thrift://172.16.9.42:9083/ctt/ratemovies")
                .setColumns(new String[]{"userid", "genres", "title"});

        // 输出库 - hbase
        HBaseConf hbaseOutputConf = new HBaseConf()
                .setPath("172.16.9.41,172.16.9.42,172.16.9.43;2181;hdfs://172.16.9.41:8020/hbase")
                .setTable("qiaobin")
                .setColumnFamily("default");

        // 输出库 - es
        ESConf esOutputConf = new ESConf()
                .setNodes("172.16.2.3")
                .setIndex("qiaobin")
                .setClusterName("es");
        try {
            StoreClient client = new PreBuilder().setMaster("local[*]")
                    .setInputSource(inputConf)
                    .setHBaseOutputSource(hbaseOutputConf)
                    .setESOutputSource(esOutputConf)
                    .client();
            JavaPairRDD<String, Map<String, String>> pairRDD =  client.readRDD();
            long ts = VersionGenerator.generateVersion();
            client.write(pairRDD, ts, "userid");
            client.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

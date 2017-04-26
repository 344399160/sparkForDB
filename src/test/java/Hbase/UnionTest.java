package Hbase;

import label.client.PreBuilder;
import label.client.StoreClient;
import label.common.VersionGenerator;
import label.model.ESConf;
import label.model.HBaseConf;
import label.model.HiveConf;
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
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

package Hbase;

import label.client.StoreClient;
import label.common.VersionGenerator;
import label.model.HBaseConf;
import label.utils.ConstUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
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
        HBaseConf inputConf = new HBaseConf()
                .setPath("172.16.9.41,172.16.9.42,172.16.9.43;2181;hdfs://172.16.9.41:8020/hbase")
                .setTable("ctt_test")
                .setColumnFamily("default")
                .setColumns(new String[]{"default:Tag"});
//        // hive 作为输入源
//        HiveConf inputConf = new HiveConf()
//                .setPath("thrift://172.16.9.42:9083/ctt/ratemovies")
//                .setColumns(new String[]{"userid", "genres", "title"});


        try {
            SparkConf sc = new SparkConf().setMaster("local[*]").setAppName("StoreClient");
            sc.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer");
            sc.set("hive.metastore.schema.verification", "false");
            sc.set("es.nodes", "172.16.2.3");
            sc.set("es.index.auto.create", "true");
            sc.set("es.port", "9200");

            sc.set(ConstUtil.HBASE_ROOTDIR, "172.16.9.41,172.16.9.42,172.16.9.43;2181;hdfs://172.16.9.41:8020/hbase");
            JavaSparkContext jsc = new JavaSparkContext(sc);
            StoreClient storeClient = new StoreClient();
            JavaPairRDD<String, Map<String, String>> pairRDD = storeClient.readRDD(jsc, inputConf, "userid");
            long ts = VersionGenerator.generateVersion();
            storeClient.write(pairRDD, ts, "qiaobin");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

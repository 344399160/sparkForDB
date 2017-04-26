package elasticsearch;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.junit.Test;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 描述：TODO
 * author qiaobin   2017/4/12 18:15.
 */
public class ElasticsearchSparkTest {

    @Test
    public void test() {
        try {
            SparkConf sc1 = null;
            sc1 = new SparkConf()
                    .setMaster("local[2]")
                    .setAppName("ElasticsearchSparkTest");
            sc1.set("es.index.auto.create", "true");
            sc1.set("es.nodes", "172.16.2.3");
            sc1.set("es.port", "9200");

            JavaSparkContext jsc1 = new JavaSparkContext(sc1);
            Map<String, String> otp = new HashMap<>();
            otp.put("tt1", "OTP");
            otp.put("tt2", "Otopeni22");

            Map<String, String> sfo = new HashMap<>();
            sfo.put("tt1", "SFO");
            sfo.put("tt2", "San Fran22");

            List<Tuple2<Object, Object>> list = new ArrayList<>();
            list.add(new Tuple2<Object, Object>(null, otp));
            list.add(new Tuple2<Object, Object>(null, sfo));

            JavaPairRDD<?, ?> pairRdd = jsc1.parallelizePairs(list);
            JavaEsSpark.saveToEsWithMeta(pairRdd, "spark/json-trips");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void union() {
        SparkConf sparkConf  = new SparkConf()
                .setMaster("local[2]").setAppName("ElasticsearchSparkTest");
        sparkConf.set("es.index.auto.create", "true");
        sparkConf.set("es.nodes", "172.16.2.3");
        sparkConf.set("es.port", "9200");
        sparkConf.set("spark.driver.allowMultipleContexts", "true");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
//        HBaseSparkService hbaseService = new HBaseSparkService("172.16.9.41,172.16.9.42,172.16.9.43;2181;hdfs://172.16.9.41:8020/hbase", "default", "ctt_test");
//        JavaPairRDD<String, Map<String, String>>  hbase = hbaseService.read(jsc, new String[]{"default:Tag"});
//
//        if (null != hbase) {
//            ElasticSearchSparkService esService = new ElasticSearchSparkService();
//            esService.saveToEsWithMeta(hbase, "spark", "json-trips");
//        }
    }

    @Test
    public void hbase() {
        try {
            SparkConf sparkConf  = new SparkConf()
                    .setMaster("local[2]").setAppName("ElasticsearchSparkTest");
            sparkConf.set("es.index.auto.create", "true");
            sparkConf.set("es.nodes", "172.16.2.3");
            sparkConf.set("es.port", "9200");
            sparkConf.set("spark.driver.allowMultipleContexts", "true");
            sparkConf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer");
            JavaSparkContext jsc = new JavaSparkContext(sparkConf);
//            HBaseSparkService hbaseService = new HBaseSparkService("172.16.9.41,172.16.9.42,172.16.9.43;2181;hdfs://172.16.9.41:8020/hbase", "default", "ctt_test");
//            JavaPairRDD<ImmutableBytesWritable, Put>  hbase = hbaseService.readPut(jsc, "a", "default");
//            JavaPairRDD<String, Map<String, String>>  hbase1 = hbaseService.read(jsc, new String[]{"default:Tag"});
//
//            if (null != hbase) {
//                HBaseOutputTable outputTable = new HBaseOutputTable("172.16.9.41,172.16.9.42,172.16.9.43;2181;hdfs://172.16.9.41:8020/hbase", "default", "qiaobin");
//                outputTable.writeFromHbase(hbase);
//            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

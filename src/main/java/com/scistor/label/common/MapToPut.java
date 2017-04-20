package com.scistor.label.common;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Iterator;
import java.util.Map;

/**
 * 描述：Map 转 Put
 * author qiaobin   2017/4/14 15:48.
 */
public class MapToPut implements PairFunction<Tuple2<String, Map<String, String>>, ImmutableBytesWritable, Put> {
    private String columnFamily;
    private long timestamp;

    public MapToPut(String columnFamily, long timestamp) {
        this.columnFamily = columnFamily;
        this.timestamp = timestamp;
    }

    @Override
    public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, Map<String, String>> tuple2) throws Exception {
        Map<String, String> map = tuple2._2();
        Put p = new Put(Bytes.toBytes(tuple2._1()));
        Iterator it = map.keySet().iterator();
        String column;
        while (it.hasNext()){
            column = it.next().toString();
            p.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), timestamp, Bytes.toBytes(map.get(column)));
        }
        return new Tuple2<>(new ImmutableBytesWritable(), p);
    }
}

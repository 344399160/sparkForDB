package com.scistor.label.common;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Map;

/**
 * 描述：Tuple中的第一个参数是Hbase的rowkey,因此需要将其取出作为一个字段保存
 * author qiaobin   2017/4/14 15:48.
 */
public class MapToESMapWithRowKeyColumn implements PairFunction<Tuple2<String, Map<String, String>>, String, Map<String, String>> {

    private long timestamp;
    private String rowKeyColumn;

    public MapToESMapWithRowKeyColumn(long timestamp, String rowKeyColumn) {
        this.timestamp = timestamp;
        this.rowKeyColumn = rowKeyColumn;
    }

    @Override
    public Tuple2<String, Map<String, String>> call(Tuple2<String, Map<String, String>> tuple2) throws Exception {
        Map<String, String> map = tuple2._2();
        map.put("rowKey", map.get(rowKeyColumn));
        map.put("version", String.valueOf(timestamp));
        return new Tuple2<String, Map<String, String>>(null, map);
    }
}

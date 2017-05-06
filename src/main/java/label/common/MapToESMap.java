package label.common;

import label.utils.ConstUtil;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Map;

/**
 * 描述：Tuple中的第一个参数是Hbase的rowkey,因此需要将其取出作为一个字段保存
 * author qiaobin   2017/4/14 15:48.
 */
public class MapToESMap implements PairFunction<Tuple2<String, Map<String, String>>, String, Map<String, String>> {

    private long timestamp;

    public MapToESMap(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public Tuple2<String, Map<String, String>> call(Tuple2<String, Map<String, String>> tuple2) throws Exception {
        String rowKey = tuple2._1;
        Map<String, String> map = tuple2._2();
        map.put(ConstUtil.ROWKEY, rowKey);
        map.put(ConstUtil.VERSION, String.valueOf(timestamp));
        return new Tuple2<String, Map<String, String>>(null, map);
    }
}

package label.common;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

/**
 * 描述：Result 转 Map
 * author qiaobin   2017/4/13 15:40.
 */
public class ResultToMap implements PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, Map<String, String>> {

    @Override
    public Tuple2<String,  Map<String, String>> call(Tuple2<ImmutableBytesWritable, Result> tuple) throws Exception {
        Map<String, String> map = new HashMap<String, String>();
        Result result = tuple._2();
        Cell[] cells = result.rawCells();
        for(Cell cell:cells){
            map.put(new String(CellUtil.cloneQualifier(cell)), new String(CellUtil.cloneValue(cell)));
        }
        return new Tuple2<String, Map<String, String>>(Bytes.toString(CellUtil.cloneRow(cells[0])), map);
    }
}

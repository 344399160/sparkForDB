package label.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.List;

/**
 * Created by ctt on 2016/11/8.
 */
public class ResultToPut implements PairFunction<Tuple2<ImmutableBytesWritable, Result>, ImmutableBytesWritable, Put> {
    private final Logger logger = LoggerFactory.getLogger(ResultToPut.class);

    private long timestamp;
    private String columnFamily;

    public ResultToPut(long timestamp, String columnFamily) {
        this.timestamp = timestamp;
        this.columnFamily = columnFamily;
    }

    public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<ImmutableBytesWritable, Result> tuple2) throws Exception {
        List<Cell> result = tuple2._2().listCells();
        Put p = null;
        for (Cell cell:result) {
            p = new Put(CellUtil.cloneRow(cell));
            p.addColumn(Bytes.toBytes(columnFamily), CellUtil.cloneQualifier(cell), System.currentTimeMillis() ,CellUtil.cloneValue(cell));
        }
        return new Tuple2<ImmutableBytesWritable, Put>(tuple2._1(),p);
    }
}

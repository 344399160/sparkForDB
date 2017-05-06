package label.common;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.spark.SerializableWritable;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by ctt on 2016/11/6.
 * 将HCatRecord结构的RDD转为Put结构的RDD，方便向hbase存储数据
 */
public class HCatToMap implements PairFunction<Tuple2<WritableComparable, SerializableWritable<HCatRecord>>, String,  Map<String, String>> {
    private HCatSchema hCatSchema;
    private String keyLabelName;

    public HCatToMap(HCatSchema hCatSchema, String keyLabelName){
        this.hCatSchema = hCatSchema;
        this.keyLabelName = keyLabelName;
    }
    public Tuple2<String,  Map<String, String>> call(Tuple2<WritableComparable, SerializableWritable<HCatRecord>> tuple2) throws Exception {
        Map<String, String> map = new HashMap<String, String>();
        HCatRecord record = tuple2._2().value();
        List<String> fieldName = hCatSchema.getFieldNames();
        String rowKey = "";
        for (int i = 0; i < fieldName.size(); i++) {
            if (fieldName.get(i).equals(keyLabelName)) {
                rowKey = record.get(fieldName.get(i),hCatSchema).toString();
            }
            map.put(fieldName.get(i), record.get(fieldName.get(i),hCatSchema).toString());
        }
        return new Tuple2<String,  Map<String, String>>(rowKey, map);
    }
}

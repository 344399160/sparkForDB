package label.utils;

import label.model.Entity;
import label.model.LabelResult;
import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import java.util.*;

/**
 * 描述：表查询数据封装
 * author qiaobin   2017/4/26 11:10.
 */
public class ResultFormatter {

    //--------------------HBASE
    /**
     * 功能描述：查询结果格式化
     * @param result
     * @return map Map<column, value>
     */
    public static LabelResult formatResult(Result result, String rowKey){
        LabelResult labelResult = new LabelResult();
        List<Entity> entityList = new ArrayList<>();
        Cell[] cells = result.rawCells();
        Map<String, Map<String, Object>> sortObjMap = new HashMap<>();
        for(Cell cell:cells){
            Map<String, Object> keyVal;
            if (null == sortObjMap.get(cell.getTimestamp() + "")) {
                keyVal = new HashMap<>();
            } else {
                keyVal = sortObjMap.get(cell.getTimestamp() + "");
            }
            keyVal.put(new String(CellUtil.cloneQualifier(cell)), new String(CellUtil.cloneValue(cell)));
            sortObjMap.put(cell.getTimestamp() + "", keyVal);
        }
        if (sortObjMap.size() > 0) {
            for (String ts : sortObjMap.keySet()) {
                Entity entity = new Entity();
                entity.setRowKey(rowKey);
                entity.setRowData(sortObjMap.get(ts+""));
                entity.setVersion(Long.parseLong(ts));
                entityList.add(entity);
            }
        }
        labelResult.setEntityList(entityList);
        labelResult.setSource(ConstUtil.HBASE);
        return labelResult;
    }

    /**
     * 功能描述：查询结果格式化
     * @param results
     * @return map Map<column, value>
     */
    public static LabelResult formatResults(Result[] results){
        Map<String, Map<String, Object>> sortObjMap = new HashMap<>();
        LabelResult labelResult = new LabelResult();
        List<Entity> entityList = new ArrayList<>();
        for (Result result : results) {
            Map<String, Object> map = new HashMap<>();
            String rowKey = "";
            long version;
            Cell[] cells = result.rawCells();
            for(Cell cell:cells){
                rowKey = Bytes.toString(CellUtil.cloneRow(cell));
                version = cell.getTimestamp();
                Map<String, Object> keyVal;
                if (null == sortObjMap.get(version + "")) {
                    keyVal = new HashMap<>();
                } else {
                    keyVal = sortObjMap.get(version + "");
                }
                keyVal.put(new String(CellUtil.cloneQualifier(cell)), new String(CellUtil.cloneValue(cell)));
                sortObjMap.put(version + "", keyVal);
            }
            if (sortObjMap.size() > 0) {
                for (String ts : sortObjMap.keySet()) {
                    Entity entity = new Entity();
                    entity.setRowKey(rowKey);
                    entity.setRowData(sortObjMap.get(ts+""));
                    entity.setVersion(Long.parseLong(ts));
                    entityList.add(entity);
                }
            }
            labelResult.setEntityList(entityList);
            labelResult.setSource(ConstUtil.HBASE);
        }
        return labelResult;
    }

    /**
     * 功能描述：查询结果格式化
     * @param results
     * @return map Map<column, value>
     */
    public static LabelResult formatResults(ResultScanner results, int pageSize){
        LabelResult labelResult = new LabelResult();
        List<Entity> entityList = new ArrayList<>();
        int count = 1;
        long version;
        for (Result result : results) {
            String rowKey = "";
            Cell[] cells = result.rawCells();
            String startRow = "";
            if (count > pageSize) {
                startRow = Bytes.toString(CellUtil.cloneRow(cells[0]));
                labelResult.setStartRow(startRow);
                break;
            } else {
                Map<String, Map<String, Object>> sortObjMap = new HashMap<>();
                for (int i = 0; i < cells.length; i++) {
                    rowKey = Bytes.toString(CellUtil.cloneRow(cells[i]));
                    version = cells[i].getTimestamp();
                    Map<String, Object> keyVal;
                    if (null == sortObjMap.get(version + "")) {
                        keyVal = new HashMap<>();
                    } else {
                        keyVal = sortObjMap.get(version + "");
                    }
                    keyVal.put(new String(CellUtil.cloneQualifier(cells[i])), new String(CellUtil.cloneValue(cells[i])));
                    sortObjMap.put(version + "", keyVal);
                }
                if (sortObjMap.size() > 0) {
                    for (String ts : sortObjMap.keySet()) {
                        Entity entity = new Entity();
                        entity.setRowKey(rowKey);
                        entity.setRowData(sortObjMap.get(ts+""));
                        entity.setVersion(Long.parseLong(ts));
                        entityList.add(entity);
                    }
                }

            }
            count ++;
        }
        //得到获取条数终止循环
        labelResult.setEntityList(entityList);
        labelResult.setSource(ConstUtil.HBASE);
        return labelResult;
    }

    //--------------------ES
    /**
     * 功能描述：ES 统计方法
     */
    public final static Map<String, Object> formatRecord(Terms terms) {
        Map<String, Object> map = new HashedMap();
        Iterator<Terms.Bucket> iter = terms.getBuckets().iterator();

        while (iter.hasNext()) {
            Terms.Bucket gradeBucket = iter.next();
            map.put(gradeBucket.getKey().toString(), gradeBucket.getDocCount());
        }
        return map;
    }

    public final static LabelResult formatRecord(SearchHits hits) {
        LabelResult result = new LabelResult();
        List<Entity> entityList = new ArrayList<>();
        SearchHit[] searchHists = hits.getHits();
        long version = 0;
        if (searchHists.length > 0) {
            version = Long.parseLong(searchHists[0].getSource().get(ConstUtil.VERSION).toString());
        }
        for (SearchHit sh : searchHists) {
            Map<String, Object> map = sh.getSource();
            String rowKey = map.get(ConstUtil.ROWKEY).toString();
            Entity entity = new Entity();
            entity.setRowKey(rowKey);
            entity.setRowData(map);
            entity.setEsId(sh.getId());
            entity.setType(sh.getType());
            entity.setVersion(version);
            entityList.add(entity);
        }
        result.setSource(ConstUtil.ES);
        result.setEntityList(entityList);
        return result;
    }
}

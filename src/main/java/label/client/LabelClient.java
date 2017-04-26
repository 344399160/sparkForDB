package label.client;

import label.common.MessageException;
import label.elasticsearch.ESQueryConstructor;
import label.elasticsearch.ESService;
import label.hbase.HBaseService;
import label.model.LabelResult;
import label.utils.BeanUtil;
import label.utils.ConstUtil;
import label.utils.Json;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * 描述：ES、Hbase 适配操作
 * author qiaobin   2017/4/25 14:31.
 */
public class LabelClient {

    private ESService esService;
    private HBaseService hBaseService;

    public void init() {
        this.esService = new ESService("es", "172.16.2.3", 9300);
        this.hBaseService = new HBaseService("172.16.9.42", "2181");
    }

    private void check() {
        if (null == this.esService && null == this.hBaseService) {
            throw new MessageException("至少应有一种数据库配置信息！");
        }
    }

    /**
     * 功能描述：建表
     * @author qiaobin
     * @date 2017/4/25  14:33
     * @param tableName 表名
     */
    public void createTabel(String tableName) {
        this.check();
        if (null != this.esService) {
            this.esService.createIndex(tableName);
        }
        if (null != this.esService) {
            this.hBaseService.createTable(tableName, "");
        }
    }

    /**
     * 功能描述：删除表
     * @author qiaobin
     * @date 2017/4/25  14:33
     * @param tableName 表名
     */
    public void dropTabel(String tableName) {
        this.check();
        if (null != this.esService) {
            this.esService.deleteIndex(tableName);
        }
        if (null != this.esService) {
            this.hBaseService.dropTable(tableName);
        }
    }

    /**
     * 功能描述：禁用表
     * @author qiaobin
     * @date 2017/4/25  17:00
     * @param tableName 表名
     */
    public void disableTabel(String tableName) {
        this.check();
        if (null != this.esService) {
            this.esService.disabledIndex(tableName);
        }
        if (null != this.hBaseService) {
            this.hBaseService.disableTable(tableName);
        }
    }

    /**
     * 功能描述：删除一行数据
     * @author qiaobin
     * @date 2017/4/25  14:52
     * @param tableName 表名
     * @param type
     * @param timestamp 版本 es删除可不填
     * @param id  id 如es有初始化此id为ES表中主键， 如没初始化该id为hbase表的rowKey
     */
    public void deleteRow(String tableName, String type, String id, long timestamp) throws IOException {
        if (null != this.esService) {
            if (!StringUtils.isNotEmpty(type)) {
                throw new MessageException("type 不能为空！");
            }
            Map<String, Object> map = this.esService.get(tableName, type, id);
            String rowKey = map.get(ConstUtil.ROWKEY).toString();
            long version = Long.parseLong(map.get(ConstUtil.VERSION).toString());
            this.hBaseService.deleteRow(tableName, rowKey, version);
            this.esService.deleteData(tableName, type, id);
        }
        if (null != this.esService) {
            this.hBaseService.deleteRow(tableName, id, timestamp);
        }
    }

    /**
     * 功能描述：删除多条数据
     * @author qiaobin
     * @date 2017/4/25  14:52
     * @param tableName 表名
     * @param type
     * @param timestamp 版本 es删除可不填
     * @param ids  id 如es有初始化此id为ES表中主键， 如没初始化该id为hbase表的rowKey
     */
    public void bulkDeleteRow(String tableName, String type, Collection<String> ids, long timestamp) throws IOException {
        if (null != this.esService) {
            List<String> hbaseRowKeys = new ArrayList<>();
            if (!StringUtils.isNotEmpty(type)) {
                throw new MessageException("type 不能为空！");
            }
            List<Map<String, Object>> maps = this.esService.get(tableName, type, ids);
            for (int i = 0; i < maps.size(); i++) {
                Map<String, Object> data = maps.get(i);
                hbaseRowKeys.add(data.get(ConstUtil.ROWKEY).toString());
            }
            this.esService.bulkDeleteData(tableName, type, ids);
            this.hBaseService.batchDeleteRow(tableName, hbaseRowKeys, Long.parseLong(maps.get(0).get(ConstUtil.VERSION).toString()));
        }
        if (null != this.esService) {
            this.hBaseService.batchDeleteRow(tableName, ids, timestamp);
        }
    }

    /**
     * 功能描述：插入数据
     * @author qiaobin
     * @date 2017/4/25  15:23
     * @param tableName 表名
     * @param rowKey 主键（该主键是hbase的rowKey， es的主键依旧是随机值）
     * @param data 数据<字段， 值>
     * @param timestamp 版本库生成的时间戳
     */
    public void insertRow(String tableName, String rowKey, Map<String, Object> data, long timestamp) throws Exception {
        if (null != this.esService) {
            String type = this.esService.generateType(tableName, timestamp);
            data.put(ConstUtil.ROWKEY, rowKey);
            data.put(ConstUtil.VERSION, String.valueOf(timestamp));
            this.esService.insertData(tableName, type, Json.toJsonString(data));
        }
        if (null != this.esService) {
            this.hBaseService.insertRow(tableName, rowKey, null, data, timestamp);
        }
    }

    /**
     * 功能描述：数据更新
     * @author qiaobin
     * @date 2017/4/25  15:48
     * @param tableName 表名
     * @param type es的type, 如果没有初始化ES，type可以不填   (非必填)
     * @param id 如es有初始化此id为ES表中主键， 如没初始化该id为hbase表的rowKey
     * @param data 要修改的数据
     * @param timestamp 如果没有初始化ES，需指定数据时间戳   (非必填)
     */
    public void updateRow(String tableName, String type, String id, Map<String, Object> data, long timestamp) throws Exception {
        if (null != this.esService) {   //如果es作为检索库，先取出该条数据，根据数据的rowkey和timestamp取出hbase中相对数据，并对数据修改重新保存
            Map<String, Object> map = this.esService.get(tableName, type, id);
            if (map.size() <= 0) {
                throw new MessageException(MessageFormat.format("id :{0} 不存在", id));
            }
            long version;
            String rowKey = "";
            try {
                version = Long.parseLong(map.get(ConstUtil.VERSION).toString());
                rowKey = map.get(ConstUtil.ROWKEY).toString();
            } catch (NullPointerException e) {
                throw new MessageException("请确认库中是否有version, rowKey字段，且字段不为空");
            }
            LabelResult labelResult = this.hBaseService.getRowData(tableName, rowKey, ConstUtil.COLUMNFAMILY_DEFAULT, version);
            Map<String, Object> changedRow = BeanUtil.combineObject(labelResult.getRowData(), data);

            this.esService.updateData(tableName, type, id, Json.toJsonString(data));
            this.hBaseService.insertRow(tableName, rowKey, ConstUtil.COLUMNFAMILY_DEFAULT, changedRow, labelResult.getTimestamp());
        } else {
            LabelResult labelResult = this.hBaseService.getRowData(tableName, id, ConstUtil.COLUMNFAMILY_DEFAULT, timestamp);
            Map<String, Object> changedRow = BeanUtil.combineObject(labelResult.getRowData(), data);
            this.hBaseService.insertRow(tableName, id, ConstUtil.COLUMNFAMILY_DEFAULT, changedRow, labelResult.getTimestamp());
        }
    }

    /**
     * 功能描述：ES检索
     * @author qiaobin
     * @date 2017/4/25  17:55
     * @param tableName 表名
     * @param constructor 查询构造
     * @param timestamp 版本
     */
    public LabelResult searchFromES(String tableName, ESQueryConstructor constructor, long timestamp) {
        String[] types = this.esService.getSameTypes(tableName, timestamp);
        if (null == types) {
            throw new MessageException("没有可用于检索的type");
        }
        List<Map<String, Object>> esDataList = this.esService.search(tableName, constructor, types);
        return null;
    }

    /**
     * 功能描述：根据rowkey查询hbase数据
     * @author qiaobin
     * @date 2017/4/25  17:53
     * @param tableName 表名
     * @param rowKeys
     * @param timestamp 版本
     */
    public List<Map<String,String>> searchByRowKeys(String tableName, Collection<String> rowKeys, long timestamp) throws IOException{
        return this.hBaseService.getRowData(tableName, rowKeys, ConstUtil.COLUMNFAMILY_DEFAULT, timestamp);
    }



}

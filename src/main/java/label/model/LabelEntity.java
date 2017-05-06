package label.model;

import java.util.Map;

/**
 * 描述：标签实体-用于存储
 * author qiaobin   2017/5/2 16:33.
 */
public class LabelEntity {

    //rowkey
    private String rowKey;
    //数据
    private Map<String, Object> entity;

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public Map<String, Object> getEntity() {
        return entity;
    }

    public void setEntity(Map<String, Object> entity) {
        this.entity = entity;
    }
}

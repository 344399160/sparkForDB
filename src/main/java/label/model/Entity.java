package label.model;

import java.util.Map;

/**
 * 描述：数据实体
 * author qiaobin   2017/4/27 13:52.
 */
public class Entity {

    //唯一主键
    private String rowKey;

    //版本
    private long version;

    //数据集
    private Map<String, Object> rowData;

    //es type
    private String type;

    //ES主键， 只在ES检索时有该值
    private String esId;

    public String getEsId() {
        return esId;
    }

    public void setEsId(String esId) {
        this.esId = esId;
    }

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public Map<String, Object> getRowData() {
        return rowData;
    }

    public void setRowData(Map<String, Object> rowData) {
        this.rowData = rowData;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }
}

package label.model;

import java.util.Map;

/**
 * 描述：数据集
 * author qiaobin   2017/4/25 16:34.
 */
public class LabelResult {

    //时间戳
    private long timestamp;

    //数据集
    private Map<String, Object> rowData;

    //唯一主键
    private String rowKey;

    public Map<String, Object> getRowData() {
        return rowData;
    }

    public void setRowData(Map<String, Object> rowData) {
        this.rowData = rowData;
    }

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}

package label.model;

import java.util.List;

/**
 * 描述：数据集
 * author qiaobin   2017/4/25 16:34.
 */
public class LabelResult {

    //数据源区分 （ES, HBASE）
    private String source;

    //HBASE分页查询起始rowkey
    private String startRow;

    //数据集
    private List<Entity> entityList;

    public List<Entity> getEntityList() {
        return entityList;
    }

    public void setEntityList(List<Entity> entityList) {
        this.entityList = entityList;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getStartRow() {
        return startRow;
    }

    public void setStartRow(String startRow) {
        this.startRow = startRow;
    }
}

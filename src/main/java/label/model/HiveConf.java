package label.model;

import label.common.MessageException;
import org.apache.commons.lang.StringUtils;

/**
 * 描述：Hive连接信息
 * author qiaobin   2017/4/14 10:19.
 */
public class HiveConf extends AbstractConf {

    //连接地址
    private String path;

    //字段 （hbase格式：列族:字段）
    private String[] columns;

    //检验字段是否有空
    public HiveConf check() {
        if (StringUtils.isNotEmpty(path) && columns.length > 0)
            return this;
        else
            throw new MessageException("params [path, columns] can not be null！");
    }

    public String getPath() {
        return path;
    }

    public HiveConf setPath(String path) {
        this.path = path;
        return this;
    }

    public String[] getColumns() {
        return columns;
    }

    public HiveConf setColumns(String[] columns) {
        this.columns = columns;
        return this;
    }
}

package com.scistor.label.model;

import com.scistor.label.common.MessageException;
import org.apache.commons.lang.StringUtils;

/**
 * 描述：HBase 连接信息
 * author qiaobin   2017/4/14 10:18.
 */
public class HBaseConf extends AbstractConf {

    //表
    private String table;

    //列族
    private String columnFamily = "default";

    //连接地址
    private String path;

    //字段 （hbase格式：列族:字段）
    private String[] columns;

    //检验字段是否有空
    public HBaseConf check() {
        if (StringUtils.isNotEmpty(path) && StringUtils.isNotEmpty(table) && StringUtils.isNotEmpty(columnFamily))
            return this;
        else
            throw new MessageException("params [path, table, columnFamily] can not be null！");
    }

    public String getPath() {
        return path;
    }

    public HBaseConf setPath(String path) {
        this.path = path;
        return this;
    }

    public String[] getColumns() {
        return columns;
    }

    public HBaseConf setColumns(String[] columns) {
        this.columns = columns;
        return this;
    }

    public String getTable() {
        return table;
    }

    public HBaseConf setTable(String table) {
        this.table = table;
        return this;
    }

    public String getColumnFamily() {
        return columnFamily;
    }

    public HBaseConf setColumnFamily(String columnFamily) {
        this.columnFamily = columnFamily;
        return this;
    }

}

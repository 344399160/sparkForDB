package com.scistor.label.hbase;

import com.scistor.label.common.MessageException;
import com.scistor.label.utils.ConstUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;

import java.io.IOException;
import java.util.*;

/**
 * Hbase 通用接口
 */
public class HBaseService {

    @Autowired
    private Environment environment;

    public static Configuration conf;
    public static Connection connection;
    public static Admin admin;

    private final static String ROWKEY = "rowKey";

    public HBaseService(String ip, String port){
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", port);
        conf.set("hbase.zookeeper.quorum", ip);
        try {
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
        } catch (IOException e) {
            throw new MessageException("Hbase连接初始化错误", e);
        }
    }

    /**
     * 功能描述：关闭连接
     * @author qiaobin
     */
    public static void close(){
        try {
            if(null != admin)
                admin.close();
            if(null != connection)
                connection.close();
        } catch (IOException e) {
            throw new MessageException("关闭失败", e);
        }

    }

    /**
     * 创建HBase表
     * @param table          指定表名
     * @param columnFamilies 初始化的列簇，可以指定多个
     */
    public void createTable(String table, Collection<String> columnFamilies) {
        TableName tableName = TableName.valueOf(table);
        try {
            if (admin.tableExists(tableName)) {
                throw new MessageException("该表已存在");
            } else {
                HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
                for (String col : columnFamilies) {
                    HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(col);
                    hColumnDescriptor.setMaxVersions(ConstUtil.MAX_VERSION);
                    hTableDescriptor.addFamily(hColumnDescriptor);
                }
                admin.createTable(hTableDescriptor);
            }
        } catch (IOException e) {
            throw new MessageException("建表失败", e);
        }
    }

    /**
     * 创建HBase表
     * @param table          指定表名
     * @param columnFamily 初始化的列簇，可以指定多个
     */
    public void createTable(String table, String columnFamily) {
        if (!StringUtils.isNotEmpty(columnFamily)) {
            columnFamily = environment.getProperty("hbase.columnfamily.name.default");
        }
        TableName tableName = TableName.valueOf(table);
        try {
            if (admin.tableExists(tableName)) {
                throw new MessageException("该表已存在");
            } else {
                HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
                    HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(columnFamily);
                    hColumnDescriptor.setMaxVersions(ConstUtil.MAX_VERSION);
                    hTableDescriptor.addFamily(hColumnDescriptor);
                admin.createTable(hTableDescriptor);
            }
        } catch (IOException e) {
            throw new MessageException("建表失败", e);
        }
    }

    /**
     * 删除HBase表
     * @param table 表名
     */
    public void dropTable(String table) {
        TableName tableName = TableName.valueOf(table);
        try {
            if (admin.tableExists(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }
        } catch (IOException e) {
            throw new MessageException("删除表失败", e);
        }
    };

    /**
     * 禁用HBase表
     * @param table 表名
     */
    public void disableTable(String table) {
        TableName tableName = TableName.valueOf(table);
        try {
            if (admin.tableExists(tableName)) {
                admin.disableTable(tableName);
            }
        } catch (IOException e) {
            throw new MessageException("关闭表失败", e);
        }
    };

    /**
     * 更新表名
     * @param oldTableName  原表名
     * @param newTableName  新表名
     */
    public void changeTableName(String oldTableName, String newTableName){
        TableName oldTable = TableName.valueOf(oldTableName);
        TableName newTable = TableName.valueOf(newTableName);
        try {
            Admin admin = connection.getAdmin();
            String snapshotName = "snapshot";
            admin.disableTable(oldTable);
            admin.snapshot(snapshotName, oldTable);
            admin.cloneSnapshot(snapshotName, newTable);
            admin.deleteSnapshot(snapshotName);
            admin.deleteTable(oldTable);
        } catch (Exception e) {
            throw new MessageException("表名修改失败", e);
        }
    }

    /**
     * 功能描述：返回所有表名
     */
    public List<String> listTableNames(){
        List<String> list = new ArrayList<String>();
        try {
            HTableDescriptor hTableDescriptors[] = admin.listTables();
            for(HTableDescriptor hTableDescriptor :hTableDescriptors){
                list.add(hTableDescriptor.getNameAsString());
            }
        } catch (IOException e) {
            throw new MessageException("获取表名列表失败", e);
        }
        return list;
    }

    /**
     * 给指定的表添加数据
     * @param tableName     表名
     * @param rowKey         行健
     * @param columnFamily  列族
     * @param column         字段
     * @param value           值
     */
    public void insertRow(String tableName, String rowKey, String columnFamily, String column, String value, long timestamp) throws IOException {
        TableName tablename = TableName.valueOf(tableName);
        Table table = connection.getTable(tablename);
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), timestamp, Bytes.toBytes(value));
        table.put(put);
    }

    /**
     * 给指定的表批量添加数据
     * @param tableName     表名
     * @param rowKey         行健
     * @param columnFamily  列族
     * @param colValue       字段-值
     */
    public void insertRow(String tableName, String rowKey, String columnFamily, Map<String, String> colValue, long timestamp) throws IOException {
        TableName tablename = TableName.valueOf(tableName);
        Table table = connection.getTable(tablename);
        Put put = new Put(Bytes.toBytes(rowKey));
        for (String key : colValue.keySet()) {
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(key), timestamp, Bytes.toBytes(colValue.get(key)));
        }
        table.put(put);
    }

    /**
     * 删除数据
     * @param tableName    表名
     * @param rowKey       行健
     */
    public void deleteRow(String tableName, String rowKey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);
    }

    /**
     * 批量删除数据
     * @param tableName    表名
     * @param rowKeys       行健
     */
    public void batchDeleteRow(String tableName, Collection<String> rowKeys) throws IOException {
        List<Delete> deletes = new ArrayList<Delete>();
        Table table = connection.getTable(TableName.valueOf(tableName));
        for (String rowKey : rowKeys) {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            deletes.add(delete);
        }
        table.delete(deletes);
    }

    /**
     * 精确到列的检索
     * @param tableName     表名
     * @param rowKey         行健
     * @param columnfamily  列族
     * @param column         字段
     */
    public Map<String, String> getColumnRowData(String tableName, String rowKey, String columnfamily, String column) throws IOException{
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        if (StringUtils.isNotEmpty(columnfamily) && StringUtils.isNotEmpty(column)) {
            get.addFamily(Bytes.toBytes(columnfamily)); //指定列族
            get.addColumn(Bytes.toBytes(columnfamily),Bytes.toBytes(column));  //指定列
        }
        Result result = table.get(get);
        return formatResult(result, rowKey);
    }

    /**
     * 列族下数据
     * @param tableName     表名
     * @param rowKey         行健
     * @param columnFamily  列族
     */
    public Map<String, String> getRowData(String tableName, String rowKey, String columnFamily, long timestamp) throws IOException{
        if (!StringUtils.isNotEmpty(columnFamily)) {
            columnFamily = environment.getProperty("hbase.columnfamily.name.default");
        }
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        if (StringUtils.isNotEmpty(columnFamily)) {
            get.addFamily(Bytes.toBytes(columnFamily)); //指定列族
        }
        get.setTimeStamp(timestamp);
        Result result = table.get(get);
        return formatResult(result, rowKey);
    }

    /**
     * 列族下数据
     * @param tableName     表名
     * @param rowKeys         行健
     * @param columnFamily  列族
     */
    public List<Map<String, String>> getRowData(String tableName, Collection<String> rowKeys, String columnFamily, long timestamp) throws IOException{
        if (!StringUtils.isNotEmpty(columnFamily)) {
            columnFamily = environment.getProperty("hbase.columnfamily.name.default");
        }
        Table table = connection.getTable(TableName.valueOf(tableName));
        List<Get> gets = new ArrayList<Get>();
        for (String rowKey : rowKeys) {
            Get get = new Get(Bytes.toBytes(rowKey));
            if (StringUtils.isNotEmpty(columnFamily)) {
                get.addFamily(Bytes.toBytes(columnFamily)); //指定列族
                get.setTimeStamp(timestamp);
            }
            gets.add(get);
        }
        Result[] result = table.get(gets);
        return formatResults(result);
    }

    /**
     * 功能描述：查询结果格式化
     * @param result
     * @return map Map<column, value>
     */
    public Map<String, String> formatResult(Result result, String rowKey){
        Map<String, String> map = new HashMap<String, String>();
        Cell[] cells = result.rawCells();
        for(Cell cell:cells){
            map.put(new String(CellUtil.cloneQualifier(cell)), new String(CellUtil.cloneValue(cell)));
        }
        if (map.size() > 0) {
            map.put(ROWKEY, rowKey);
        }
        return map;
    }

    /**
     * 功能描述：查询结果格式化
     * @param results
     * @return map Map<column, value>
     */
    public List<Map<String, String>> formatResults(Result[] results){
        List<Map<String, String>> list = new ArrayList<Map<String, String>>();
        for (Result result : results) {
            Map<String, String> map = new HashMap<String, String>();
            int i = 0;
            String rowKey = "";
            Cell[] cells = result.rawCells();
            for(Cell cell:cells){
                if (i == 0) rowKey = Bytes.toString(CellUtil.cloneRow(cell));
                map.put(new String(CellUtil.cloneQualifier(cell)), new String(CellUtil.cloneValue(cell)));
                i++;
            }
            if (map.size() > 0) {
                map.put(ROWKEY, rowKey);
                list.add(map);
            }
        }
        return list;
    }

}

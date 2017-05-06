package label.hbase;

import label.combine.SearchConstructor;
import label.common.MessageException;
import label.model.LabelEntity;
import label.model.LabelResult;
import label.utils.ConstUtil;
import label.utils.ResultFormatter;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CollectionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Hbase 通用接口
 */
public class HBaseClient {

    public static Configuration conf;
    public static Connection connection;
    public static Admin admin;

    private final static String ROWKEY = "rowKey";

    public HBaseClient(String ip, String port){
        conf = HBaseConfiguration.create();
        conf.set(ConstUtil.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT, port);
        conf.set(ConstUtil.HBASE_ZOOKEEPER_QUORUM, ip);

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
            columnFamily = ConstUtil.COLUMNFAMILY_DEFAULT;
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
     * 给指定的表添加数据
     * @param tableName     表名
     * @param rowKey         行健
     * @param columnFamily  列族
     * @param colValue       字段-值
     */
    public void insertRow(String tableName, String rowKey, String columnFamily, Map<String, Object> colValue, long timestamp) throws IOException {
        if (!StringUtils.isNotEmpty(columnFamily)) {
            columnFamily = ConstUtil.COLUMNFAMILY_DEFAULT;
        }
        TableName tablename = TableName.valueOf(tableName);
        Table table = connection.getTable(tablename);
        Put put = new Put(Bytes.toBytes(rowKey));
        for (String key : colValue.keySet()) {
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(key), timestamp, Bytes.toBytes(colValue.get(key).toString()));
        }
        table.put(put);
    }

    /**
     * 给指定的表批量添加数据
     * @param tableName     表名
     * @param entityList    批量插入数据实体
     * @param columnFamily  列族
     */
    public void bulkInsertRow(String tableName, String columnFamily, List<LabelEntity> entityList, long version) throws IOException {
        if (!StringUtils.isNotEmpty(columnFamily)) {
            columnFamily = ConstUtil.COLUMNFAMILY_DEFAULT;
        }
        if (!CollectionUtils.isEmpty(entityList)) {
            TableName tablename = TableName.valueOf(tableName);
            Table table = connection.getTable(tablename);
            List<Put> list = new ArrayList<>();
            for (LabelEntity entity : entityList) {
                if (MapUtils.isNotEmpty(entity.getEntity())) {
                    Put put = new Put(Bytes.toBytes(entity.getRowKey()));
                    Map<String, Object> map = new HashedMap();
                    map = entity.getEntity();
                    for (String key : map.keySet()) {
                        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(key), version, Bytes.toBytes(map.get(key).toString()));
                    }
                    list.add(put);
                }
            }
            table.put(list);
        }
    }

    /**
     * 删除数据
     * @param tableName    表名
     * @param rowKey       行健
     */
    public void deleteRow(String tableName, String rowKey, long timestamp) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addFamilyVersion(Bytes.toBytes(ConstUtil.COLUMNFAMILY_DEFAULT), timestamp);
        table.delete(delete);
    }

    /**
     * 批量删除数据
     * @param tableName    表名
     * @param rowKeys       行健
     */
    public void batchDeleteRow(String tableName, Collection<String> rowKeys, long timestamp) throws IOException {
        List<Delete> deletes = new ArrayList<>();
        Table table = connection.getTable(TableName.valueOf(tableName));
        for (String rowKey : rowKeys) {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            delete.addFamilyVersion(Bytes.toBytes(ConstUtil.COLUMNFAMILY_DEFAULT), timestamp);
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
    public LabelResult getColumnRowData(String tableName, String rowKey, String columnfamily, String column) throws IOException{
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        if (StringUtils.isNotEmpty(columnfamily) && StringUtils.isNotEmpty(column)) {
            get.addFamily(Bytes.toBytes(columnfamily)); //指定列族
            get.addColumn(Bytes.toBytes(columnfamily),Bytes.toBytes(column));  //指定列
        }
        Result result = table.get(get);
        return ResultFormatter.formatResult(result, rowKey);
    }

    /**
     * 列族下数据
     * @param tableName     表名
     * @param rowKey         行健
     * @param columnFamily  列族
     */
    public LabelResult getRowData(String tableName, String rowKey, String columnFamily, long timestamp) throws IOException{
        if (!StringUtils.isNotEmpty(columnFamily)) {
            columnFamily = ConstUtil.COLUMNFAMILY_DEFAULT;
        }
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        if (StringUtils.isNotEmpty(columnFamily)) {
            get.addFamily(Bytes.toBytes(columnFamily)); //指定列族
        }
        if (timestamp != 0) {
            get.setTimeStamp(timestamp);
        } else {
            get.setMaxVersions();
        }
        Result result = table.get(get);
        return ResultFormatter.formatResult(result, rowKey);
    }

    /**
     * 列族下数据
     * @param tableName     表名
     * @param rowKeys         行健
     * @param columnFamily  列族
     */
    public LabelResult getRowData(String tableName, Collection<String> rowKeys, String columnFamily, long timestamp) throws IOException{
        if (!StringUtils.isNotEmpty(columnFamily)) {
            columnFamily = ConstUtil.COLUMNFAMILY_DEFAULT;
        }
        Table table = connection.getTable(TableName.valueOf(tableName));
        List<Get> gets = new ArrayList();
        for (String rowKey : rowKeys) {
            Get get = new Get(Bytes.toBytes(rowKey));
            if (StringUtils.isNotEmpty(columnFamily)) {
                get.addFamily(Bytes.toBytes(columnFamily)); //指定列族
                if (timestamp != 0) {
                    get.setTimeStamp(timestamp);
                } else {
                    get.setMaxVersions();
                }
            }
            gets.add(get);
        }
        Result[] result = table.get(gets);
        return ResultFormatter.formatResults(result);
    }

    /**
     * 功能描述：条件查询
     * @author qiaobin
     * @date 2017/4/26  17:00
     * @param
     */
    public LabelResult search(String tableName, SearchConstructor constructor, String startRow) throws IOException{
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        if (null != constructor) {
            if (constructor.getVersion() != 0) {
                scan.setTimeStamp(constructor.getVersion());
            } else {
                scan.setMaxVersions();
            }
            if (null != constructor.listFilters().getFilters() && constructor.listFilters().getFilters().size() > 0) {
                FilterList filterList = constructor.listFilters();
                scan.setFilter(filterList);
            }
        }
        if (StringUtils.isNotEmpty(startRow)) {
            scan.setStartRow(Bytes.toBytes(startRow));
        }
        scan.addFamily(Bytes.toBytes(ConstUtil.COLUMNFAMILY_DEFAULT));
        ResultScanner rs = table.getScanner(scan);

        return ResultFormatter.formatResults(rs, constructor.getSize());
    }

    /**
     * 功能描述：数据入库数
     * @author qiaobin
     * @date 2017/4/28  11:08
     * @param tableName 表名
     * @param startTime 起始时间
     * @param endTime 结束时间
     */
    public long count(String tableName, long startTime, long endTime) throws Exception{
        long count;
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(ConstUtil.COLUMNFAMILY_DEFAULT));
        if (startTime != 0 && endTime != 0) {
            scan.setTimeRange(startTime, endTime);
        }

        AggregationClient aggregationClient = new AggregationClient(connection.getConfiguration());
        try {
            count = aggregationClient.rowCount(table, new LongColumnInterpreter(), scan);
        } catch (Throwable throwable) {
            throw new MessageException("count method error" + throwable.getMessage());
        }
        return count;
    }


    /**
     * 功能描述：根据字段返回有该字段的记录数
     * @author qiaobin
     * @date 2017/4/28  11:08
     * @param tableName 表名
     */
    public long count(String tableName, String column) throws Exception{
        long count;
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(ConstUtil.COLUMNFAMILY_DEFAULT));
        //过滤字段
        QualifierFilter qualifierFilter = new QualifierFilter(
                CompareFilter.CompareOp.EQUAL,
                new BinaryPrefixComparator(Bytes.toBytes(column)));
        scan.setFilter(qualifierFilter);
        scan.setMaxVersions();
        AggregationClient aggregationClient = new AggregationClient(connection.getConfiguration());
        try {
            count = aggregationClient.rowCount(table, new LongColumnInterpreter(), scan);
        } catch (Throwable throwable) {
            throw new MessageException("count method error" + throwable.getMessage());
        }
        ResultScanner rs = table.getScanner(scan);
        ResultFormatter.formatResults(rs, 100);
        return count;
    }

}

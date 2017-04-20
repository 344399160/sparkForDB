package com.scistor.label.hive;

import iie.udps.common.hcatalog.SerHCatInputFormat;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.spark.SerializableWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;

/**
 * @ClassName: HCatInputTable
 * @Description: 首先对应Hive中实际存在的一张表，其次根据每一次选列或选择分区操作，生成一个临时的表模式和列信息
 * 用来表示将要通过read方法取出的RDD所对应的真实表，对HCatInputTable的任何set操作或通过重载的read方法间接选择列和分区
 * 都会对临时表模式及其对应的列信息进行修改，即只保存最近一次操作对应的临时表相关信息。
 * @Author: wutianjie
 * @CreateDate: 2015.08.10
 * @UpdateDate: 2015.08.13
 * @UpdateRemark: 添加列选择功能及注释
 * @Version: 0.1
 */
public class HCatInputTable extends HCatTable {

    private HCatSchema originSchema; // 数据库名表名所对应Hive表的表模式，初始化时确定不再更改
    private HCatSchema schema;        // 临时表模式，根据每次set方法调用动态变化，并决定read()方法输出
    private Job inputJob;             // 临时任务信息，每次调用set方法获取新的实例，用于获取set方法调用后的临时表模式
    private String[] selectedColumns;// 列选择字段名称信息，默认为原始表全体字段名
    private int[] selectedColumnIndexes;//列选择字段序号信息，默认为全体字段序号，从0开始计算

    public HCatInputTable(String metaStoreURI, String databaseName, String tableName) throws IOException {
        super(metaStoreURI, databaseName, tableName);
        initTable();
    }

    public HCatInputTable(String hCatTable) throws IOException {
        super(hCatTable);
        initTable();
    }

    // 初
    private void initTable() throws IOException {
        this.inputJob = Job.getInstance();
        SerHCatInputFormat.setInput(inputJob.getConfiguration(), this.databaseName, this.tableName);
        this.originSchema = SerHCatInputFormat.getTableSchema(inputJob.getConfiguration());
        this.schema = SerHCatInputFormat.getTableSchema(inputJob.getConfiguration());
        int[] columnIndexes = new int[originSchema.size()];
        for (int i = 0; i < originSchema.size(); i++) {
            columnIndexes[i] = i;
        }
        this.selectedColumnIndexes = columnIndexes;
        this.selectedColumns = new String[originSchema.size()];
        originSchema.getFieldNames().toArray(this.selectedColumns);
    }

    // 获取当前表模式，会根据选列等操作发生变化，值为最近一次操作所对应的表模式
    public HCatSchema getSchema() {
        return schema;
    }

    // 返回原始表模式，即当前数据库名表名所对应的Hive表的模式
    public HCatSchema getOriginSchema() {
        return originSchema;
    }

    // 获取列选择后所选列的列名，若未进行选列操作，则返回全部列列名
    public String[] getSelectedColumns() {
        return selectedColumns;
    }

    // 获取列选择后所选列对应的序号，从0开始，若未进行选列操作，则返回原始表全体字段序号
    public int[] getSelectedColumnIndexes() {
        return selectedColumnIndexes;
    }

    // 根据列名选择原始表中一个或多个字段，read操作取出的数据与选列对应
    public HCatInputTable setColumns(String[] columnNames) throws IOException {
        updateColumnInfo(columnNames);
        this.inputJob = Job.getInstance();
        SerHCatInputFormat.setInput(inputJob.getConfiguration(), this.databaseName, this.tableName, this.selectedColumns);
        this.schema = SerHCatInputFormat.getTableSchema(inputJob.getConfiguration(), selectedColumns);
        return this;
    }

    // 根据列序号选择原始表中一个或多个字段，read操作取出的数据与选列对应
    public HCatInputTable setColumns(int[] columnIndexes) throws IOException {
        updateColumnInfo(columnIndexes);
        setColumns(this.selectedColumns);
        return this;
    }

    // 根据指定分区表达式读取数据，添加分区语句时实际上并没有更改HCatInputTable中的临时表模式schema
    // 但是会覆盖数据读取任务inputJob
    public HCatInputTable setFilter(String filter) throws IOException {
        this.inputJob = Job.getInstance();
        SerHCatInputFormat.setInput(inputJob.getConfiguration(), this.databaseName, this.tableName, filter);
        this.schema = SerHCatInputFormat.getTableSchema(inputJob.getConfiguration());
        return this;
    }

    // 同时制定读取列与分区过滤表达式，只有此方法可以同时配置列选择和分区，单独调用setColumns和setFilter
    // 时，之前的set语句总会被后者覆盖
    public HCatInputTable setColumnsAndFilter(String[] columnNames, String filter) throws IOException {
        updateColumnInfo(columnNames);
        this.inputJob = Job.getInstance();
        SerHCatInputFormat.setInput(inputJob.getConfiguration(), this.databaseName, this.tableName, filter, this.selectedColumns);
        this.schema = SerHCatInputFormat.getTableSchema(inputJob.getConfiguration(), selectedColumns);
        return this;
    }

    // 同时制定读取列与分区过滤表达式，只有此方法可以同时配置列选择和分区，单独调用setColumns和setFilter
    // 时，之前的set语句总会被后者覆盖
    public HCatInputTable setColumnsAndFilter(int[] columnIndexes, String filter) throws IOException {
        updateColumnInfo(columnIndexes);
        this.inputJob = Job.getInstance();
        SerHCatInputFormat.setInput(inputJob.getConfiguration(), this.databaseName, this.tableName, filter, this.selectedColumns);
        this.schema = SerHCatInputFormat.getTableSchema(inputJob.getConfiguration(), selectedColumns);
        return this;
    }

    // 内部更新列选择信息方法
    private void updateColumnInfo(String[] columnNames) {
        if (columnNames != null) {
            this.selectedColumns = columnNames;
            int[] columnIndexes = new int[columnNames.length];
            for (int i = 0; i < columnNames.length; i++) {
                columnIndexes[i] = originSchema.getPosition(columnNames[i]);
            }
            this.selectedColumnIndexes = columnIndexes;
        }
    }

    // 内部更新列选择信息方法
    private void updateColumnInfo(int[] columnIndexes) {
        if (columnIndexes != null) {
            this.selectedColumnIndexes = columnIndexes;
            String[] columnNames = new String[columnIndexes.length];
            for (int i = 0; i < columnIndexes.length; i++) {
                columnNames[i] = originSchema.getFieldNames().get(columnIndexes[i]);
            }
            this.selectedColumns = columnNames;
        }
    }

    // 获取根据指定列生成的RDD
    public JavaPairRDD<WritableComparable, SerializableWritable<HCatRecord>> read(JavaSparkContext jsc) {
        JavaPairRDD<WritableComparable, SerializableWritable<HCatRecord>> rdd = jsc
                .newAPIHadoopRDD(inputJob.getConfiguration(),
                        SerHCatInputFormat.class, WritableComparable.class,
                        SerializableWritable.class).mapToPair(new MapToSerWriHCatRecord());
        return rdd;
    }

    // 按列名选列并取数据
    public JavaPairRDD<WritableComparable, SerializableWritable<HCatRecord>> read(JavaSparkContext jsc
            , String[] columnNames) throws IOException {
        setColumns(columnNames);
        return read(jsc);
    }

    // 按列序号选列并取数据
    public JavaPairRDD<WritableComparable, SerializableWritable<HCatRecord>> read(JavaSparkContext jsc
            , int[] columnIndexes) throws IOException {
        setColumns(columnIndexes);
        return read(jsc);
    }

    // 按分区过滤表达式选列并取数据
    public JavaPairRDD<WritableComparable, SerializableWritable<HCatRecord>> read(JavaSparkContext jsc
            , String filter) throws IOException {
        setFilter(filter);
        return read(jsc);
    }

    // 按列序号、分区过滤表达式选列并取数据
    public JavaPairRDD<WritableComparable, SerializableWritable<HCatRecord>> read(JavaSparkContext jsc
            , String[] columnNames, String filter) throws IOException {
        setColumnsAndFilter(columnNames, filter);
        return read(jsc);
    }

    // 按列序号、分区过滤表达式选列并取数据
    public JavaPairRDD<WritableComparable, SerializableWritable<HCatRecord>> read(JavaSparkContext jsc
            , int[] columnIndexes, String filter) throws IOException {
        setColumnsAndFilter(columnIndexes, filter);
        return read(jsc);
    }

    // 获取原始表所有数据生成的RDD
    public JavaPairRDD<WritableComparable, SerializableWritable<HCatRecord>> readAll(JavaSparkContext jsc) throws IOException {
        Job readAllJob = Job.getInstance();
        SerHCatInputFormat.setInput(readAllJob.getConfiguration(), this.databaseName, this.tableName);
        JavaPairRDD<WritableComparable, SerializableWritable<HCatRecord>> rdd = jsc
                .newAPIHadoopRDD(readAllJob.getConfiguration(),
                        SerHCatInputFormat.class, WritableComparable.class,
                        SerializableWritable.class).mapToPair(new MapToSerWriHCatRecord());
        return rdd;
    }

    /**
     * @ClassName: MapToSerWriHCatRecord
     * @Description: 将SerializableWritable转换为SerializableWritable<HCatRecord>
     * @Author: wutianjie
     * @CreateDate: 2015.08.13
     * @UpdateDate: 2015.08.13
     * @UpdateRemark: 解决HCatInputTable调动时任务序列化问题
     * @Version: 0.1
     */
    public static class MapToSerWriHCatRecord implements PairFunction<Tuple2<WritableComparable, SerializableWritable>,
            WritableComparable, SerializableWritable<HCatRecord>> {
        @Override
        public Tuple2<WritableComparable, SerializableWritable<HCatRecord>>
        call(Tuple2<WritableComparable, SerializableWritable> tuple2) throws Exception {
            return new Tuple2<WritableComparable, SerializableWritable<HCatRecord>>(tuple2._1(), tuple2._2());
        }
    }
}

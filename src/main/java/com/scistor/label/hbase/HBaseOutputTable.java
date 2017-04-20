package com.scistor.label.hbase;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.mapred.JobConf;
//import org.apache.hive.hcatalog.data.HCatRecord;
//import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.spark.api.java.JavaPairRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;

/**
 * Created by ctt on 2016/10/28.
 */
public class HBaseOutputTable extends HBaseTable implements Serializable{
    private final Logger logger = LoggerFactory.getLogger(HBaseOutputTable.class);
    private String nameSpace;
    private String tableName;
    private Table table;


    /**
     * 与HBase建立链接
     * @param hbaseconf
     * @param nameSpace
     * @param tableName
     */
    public HBaseOutputTable(String hbaseconf, String nameSpace, String tableName) throws IOException{
        super(hbaseconf);
        this.setNameSpace(nameSpace);
        this.setTableName(tableName);
        initTable();

    }

    public void initTable() throws IOException{
        TableName tn = TableName.valueOf(this.toString());
        table = connect.getTable(tn);

    }

    /**
     * 创建表
     * @param columnFamily
     * @return
     * @throws Exception
     */
    public HBaseOutputTable createTable(String[] columnFamily) throws Exception{
        Admin admin =this.connect.getAdmin();
        TableName tn = TableName.valueOf(nameSpace + ":" + tableName);
        HTableDescriptor desc = new HTableDescriptor(tn);
        for (int i = 0; i < columnFamily.length; i++) {
            desc.addFamily(new HColumnDescriptor(columnFamily[i]));
        }
        if(admin.tableExists(tn)){
            logger.info("table " + nameSpace + ":" + tableName + " Exists!");
            admin.disableTable(tn);
            admin.deleteTable(tn);
            logger.info("table " + nameSpace + ":" + tableName + " is deleted!");
        }

        admin.createTable(desc);
        logger.info("table " + nameSpace + ":" + tableName + " create Success!");
        return this;
    }
    /**
     * 创建表
     * @param columnFamily
     * @param maxVersion
     * @return
     * @throws Exception
     */
    public HBaseOutputTable createTable(String[] columnFamily, int maxVersion) throws Exception{
        Admin admin =this.connect.getAdmin();
        TableName tn = TableName.valueOf(nameSpace + ":" + tableName);
        HTableDescriptor desc = new HTableDescriptor(tn);
        for (int i = 0; i < columnFamily.length; i++) {
            desc.addFamily(new HColumnDescriptor(columnFamily[i]).setMaxVersions(maxVersion));
        }
        if(admin.tableExists(tn)){
            logger.info("table " + nameSpace + ":" + tableName + " Exists!");
            admin.disableTable(tn);
            admin.deleteTable(tn);
            logger.info("table " + nameSpace + ":" + tableName + " is deleted!");
        }

        admin.createTable(desc);
        logger.info("table " + nameSpace + ":" + tableName + " create Success!");
        return this;
    }


    /**
     * 将rdd写入hbase
     * @param rdd
     */
    public void writeFromHbase(JavaPairRDD<ImmutableBytesWritable, Put> rdd){
        JobConf jobConf = new JobConf(configuration,this.getClass());
        jobConf.setOutputFormat(TableOutputFormat.class);
        jobConf.set(TableOutputFormat.OUTPUT_TABLE,this.toString());
        rdd.saveAsHadoopDataset(jobConf);
    }
//    public void writeFromSet(JavaPairRDD<String,Set<String>> HrateRdd, String columnFamily, String column, long timestamp){
//        JavaPairRDD<String,String> rdd = HrateRdd
//                .mapToPair(new setToString());
//        writeFromString(rdd,columnFamily,column, timestamp);
//    }

//    public void writeFromString(JavaPairRDD<String,String> rdd, String columnFamily, String column, long timestamp){
//        JavaPairRDD<ImmutableBytesWritable, Put> rwsultRdd = rdd
//                .mapToPair(new stringToPut(columnFamily,column, timestamp));
//        writeFromHbase(rwsultRdd);
//    }

    /**
     * 将hcatSchema格式的rdd写入hbase表中
     * @param //rdd
     * @param //hCatSchema
     * @param //rowkeyColumn
     * @param //columnFamily
     * @param //timestamp
     */
//    public void writeFromHiveAll(JavaPairRDD<WritableComparable, SerializableWritable<HCatRecord>> rdd,
//                              HCatSchema hCatSchema,String rowkeyColumn,String columnFamily,long timestamp){
//
//        JavaPairRDD<ImmutableBytesWritable, Put> convertRdd = rdd.mapToPair(new HCatToMap(hCatSchema, rowkeyColumn, columnFamily,timestamp));
//        writeFromHbase(convertRdd);
//
//    }

//    /**
//     * 将hcatSchema格式的rdd写入hbase表中,仅写入标签列
//     * @param rdd
//     * @param hCatSchema
//     * @param rowkeyColumn
//     * @param columnFamily
//     * @param timestamp
//     * @param Column
//     */
//    public void writeFromHiveWithLabel(JavaPairRDD<WritableComparable, SerializableWritable<HCatRecord>> rdd,
//                                       HCatSchema hCatSchema,String tagColumn,String rowkeyColumn,String columnFamily,long timestamp,String Column){
//        JavaPairRDD<ImmutableBytesWritable, Put> convertRdd = rdd.mapToPair(new HCatToPutWithLabel(hCatSchema,tagColumn, rowkeyColumn, columnFamily,timestamp,Column));
//        writeFromHbase(convertRdd);
//    }

    public void setNameSpace(String nameSpace){
        this.nameSpace = nameSpace;
    }
    public String getNameSpace(){return nameSpace;}

    public void setTableName(String tableName){this.tableName = tableName;}
    public String getTableName(){return tableName;}
    @Override
    public String toString() {
        String s = "";
        s += nameSpace + ":" + tableName;
        return s;
    }
    public void droptable(String tableName) throws IOException{
            Admin admin = connect.getAdmin();
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
            logger.info(tableName + "is deleted!");
    }


}

package label.hbase;

import label.common.MessageException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;

/**
 * Created by ctt on 2016/10/28.
 */
public class HBaseInputTable extends HBaseTable {
    private String nameSpace;
    private String tableName;
    private Table table;

    public HBaseInputTable(String hbaseconf, String nameSpace, String tableName)throws IOException{
        super(hbaseconf);
        this.setNameSpace(nameSpace);
        this.setTableName(tableName);
        initTable();
    }

    /**
     * 参数columnFamilyAndColumn由columnFamily:column形式的列组成，用来选择参与运算的数据源
     * @param columnFamilyAndColumn
     * @return
     */
    public HBaseInputTable setColumn(String[] columnFamilyAndColumn){
        Scan scan = new Scan();
        for (int i = 0; i < columnFamilyAndColumn.length; i++) {
            scan.addFamily(Bytes.toBytes(columnFamilyAndColumn[i].split(":")[0]));
            scan.addColumn(Bytes.toBytes(columnFamilyAndColumn[i].split(":")[0]), Bytes.toBytes(columnFamilyAndColumn[i].split(":")[1]));
        }

        try {
            ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
            String ScanString = Base64.encodeBytes(proto.toByteArray());
            this.configuration.set(TableInputFormat.SCAN,ScanString);
        } catch (IOException e) {
            throw new MessageException(e);
        }
        return this;
    }

    /**
     * 设置参与运算的数据的范围
     * @param start_rowkey
     * @param stop_rowkey
     * @return
     */
    public HBaseInputTable setRowRange(String start_rowkey, String stop_rowkey){
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(start_rowkey));
        scan.setStopRow(Bytes.toBytes(stop_rowkey));
        try {
            ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
            String ScanString = Base64.encodeBytes(proto.toByteArray());
            this.configuration.set(TableInputFormat.SCAN,ScanString);
        } catch (IOException e) {
            throw new MessageException(e);
        }
        return this;
    }

    /**
     * 选取指定时间戳的数据
     * @param timeStamp
     * @return
     */
    public HBaseInputTable setTimeStamp(long timeStamp){
        Scan scan = new Scan();
        try {
            scan.setTimeStamp(timeStamp);
        } catch (IOException e) {
            throw new MessageException(e);
        }

        try {
            ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
            String ScanString = Base64.encodeBytes(proto.toByteArray());
            this.configuration.set(TableInputFormat.SCAN,ScanString);
        } catch (IOException e) {
            throw new MessageException(e);
        }
        return this;
    }

    public HBaseInputTable setTimeStampRange(long minStamp, long maxStamp){
        Scan scan = new Scan();
        try {
            scan.setTimeRange(minStamp, maxStamp);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
            String ScanString = Base64.encodeBytes(proto.toByteArray());
            this.configuration.set(TableInputFormat.SCAN,ScanString);
        } catch (IOException e) {
            throw new MessageException(e);
        }
        return this;
    }

    public void initTable()throws IOException{
        this.configuration.set(TableInputFormat.INPUT_TABLE, this.toString());
        connect = ConnectionFactory.createConnection(configuration);
        TableName tn = TableName.valueOf(this.toString());
        table = connect.getTable(tn);
    }

    /**
     * 读取hbase数据
     * @param jsc
     * @return
     */
    public JavaPairRDD<ImmutableBytesWritable, Result> read(JavaSparkContext jsc){
        JavaPairRDD<ImmutableBytesWritable, Result> rdd =
                jsc.newAPIHadoopRDD(configuration,
                        TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

        return rdd;
    }

    /**
     * 读取hbase指定列的数据
     * @param jsc
     * @param columnFamilyAndColumn
     * @return
     */
    public JavaPairRDD<ImmutableBytesWritable, Result> readSelectColumn(JavaSparkContext jsc, String[] columnFamilyAndColumn){
        this.setColumn(columnFamilyAndColumn);
              return read(jsc);
    }

    /**
     * 指定参与运算的数据范围
     * @param jsc
     * @param start_rowkey 起始行
     * @param stop_rowkey   结束行
     * @return
     */
    public JavaPairRDD<ImmutableBytesWritable,Result> readRowRange(JavaSparkContext jsc, String start_rowkey, String stop_rowkey){
        this.setRowRange(start_rowkey, stop_rowkey);
        return read(jsc);
    }

    /**
     * 指定参与运算的版本数据
     * @param jsc
     * @param timeStamp
     * @return
     */
    public JavaPairRDD<ImmutableBytesWritable,Result> readTimeStamp(JavaSparkContext jsc, long timeStamp){
        this.setTimeStamp(timeStamp);
        return read(jsc);
    }

    /**
     * 指定参与运算数据的版本范围
     * @param jsc
     * @param minStamp
     * @param maxStamp
     * @return
     */
    public JavaPairRDD<ImmutableBytesWritable,Result> readTimeStampRange(JavaSparkContext jsc, long minStamp, long maxStamp){
        this.setTimeStampRange(minStamp,maxStamp);
        return read(jsc);
    }
    /**
     * Result数据持久化到HBase表
     * @param rdd
     * @param timestamp
     * @param columnFamily
     * @return
     */
    public JavaPairRDD<ImmutableBytesWritable, Put> resultToPut(JavaPairRDD<ImmutableBytesWritable, Result> rdd, long timestamp, String columnFamily){
//        JavaPairRDD<ImmutableBytesWritable, Put> resultRdd = rdd.mapToPair(new ResultToPut(timestamp, columnFamily));

//        return resultRdd;
        return null;
    }

    /**
     * 删除hbase表
     * @param tableName
     * @throws IOException
     */
    public void droptable(String tableName) throws IOException{
        Admin admin = connect.getAdmin();
        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));
    }
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

}

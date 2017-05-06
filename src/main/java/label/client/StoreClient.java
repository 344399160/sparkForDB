package label.client;

import label.combine.SearchConstructor;
import label.common.*;
import label.elasticsearch.ESClient;
import label.elasticsearch.ESQueryBuilders;
import label.elasticsearch.ESQueryConstructor;
import label.hbase.HBaseClient;
import label.hbase.HBaseInputTable;
import label.hbase.HBaseOutputTable;
import label.hive.HCatInputTable;
import label.model.*;
import label.utils.ConstUtil;
import label.utils.Json;
import label.utils.Utils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.spark.SerializableWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.ho.yaml.Yaml;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.MessageFormat;
import java.util.*;

/**
 * 描述：上层调用方法
 * author qiaobin   2017/4/14 09:54.
 */
public class StoreClient {

    private ESClient esClient;
    private HBaseClient hBaseClient;

    /**
     * 功能描述：根据配置文件加载连接信息
     * @author qiaobin
     * @date 2017/4/28  14:29
     */
    public void init() throws Exception{
        String path = this.getClass().getClassLoader().getResource("").getPath();
        File file = new File(path + ConstUtil.PROPERTY_YML);
        if (!file.exists()) {
            throw new MessageException("数据库配置文件不存在");
        }
        Map map = Yaml.loadType(file, HashMap.class);
        //es property
        String clusterName = map.get(ConstUtil.CLUSTER_NAME).toString();
        String esNodes = map.get(ConstUtil.ES_NODES).toString();
        String esPort = map.get(ConstUtil.ES_TRANSPORT_PORT).toString();
        //hbase property
        String zookeeperQuorum = map.get(ConstUtil.HBASE_ZOOKEEPER_QUORUM).toString();
        String clientPort = map.get(ConstUtil.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT).toString();
        if (!StringUtils.isNotEmpty(zookeeperQuorum) || !StringUtils.isNotEmpty(clientPort)) {
            throw new MessageException("找不到hbase相关配置");
        }

        this.hBaseClient = new HBaseClient(zookeeperQuorum, clientPort);
        if (StringUtils.isNotEmpty(clusterName) && StringUtils.isNotEmpty(esNodes) && StringUtils.isNotEmpty(esPort)) {
            this.esClient = new ESClient(clusterName, esNodes, Integer.parseInt(esPort));
        }
//        this.esClient = new ESClient("es", "172.16.2.3", 9300);
//        this.hBaseClient = new HBaseClient("172.16.9.42,172.16.9.41,172.16.9.43", "2181");
    }

    /**
     * 功能描述：获取sparkConf配置
     * @author qiaobin
     * @date 2017/4/28  13:25
     */
    public Map<String, String> getSparkConf() throws FileNotFoundException{
        Map<String, String> conf = new HashMap<>();
        conf.put("spark.serializer", "org.apache.spark.serializer.JavaSerializer");
//        conf.put("hive.metastore.schema.verification", "false");
        String path = this.getClass().getClassLoader().getResource("").getPath();
        File file = new File(path + "property.yml");
        if (!file.exists()) {
            throw new MessageException("数据库配置不存在");
        }
        Map map = Yaml.loadType(file, HashMap.class);
        String rootDir = map.get(ConstUtil.HBASE_ROOTDIR).toString();
        String esNodes = map.get(ConstUtil.ES_NODES).toString();
        String esPort = map.get(ConstUtil.ES_PORT).toString();
        String autoCreate = map.get(ConstUtil.ES_INDEX_AUTO_CREATE).toString();

        if (StringUtils.isNotEmpty(esNodes) && StringUtils.isNotEmpty(esPort)) {
            conf.put(ConstUtil.ES_NODES, esNodes);
            conf.put(ConstUtil.ES_INDEX_AUTO_CREATE, autoCreate);
            conf.put(ConstUtil.ES_PORT, esPort);
        }

        conf.put(ConstUtil.HBASE_ROOTDIR, rootDir);
        return conf;
    }

    //服务检查。必须有一种服务被初始化
    private void check() {
        if (null == this.esClient && null == this.hBaseClient) {
            throw new MessageException("至少应有一种数据库配置信息！");
        }
    }

    /**
     * 功能描述：从数据源读取数据
     * @author qiaobin
     */
    public JavaPairRDD<String, Map<String, String>> readRDD(JavaSparkContext jsc, AbstractConf inputDBConf, String keyLabelName) throws Exception{
        JavaPairRDD<String, Map<String, String>> pairRDD = null;
        if (inputDBConf instanceof HiveConf) {  //从hive表中获取数据
            HiveConf hiveConf = (HiveConf) inputDBConf;
            hiveConf.check(); //检查必填值是否有空值
            HCatInputTable hcatInputTable = new HCatInputTable(hiveConf.getPath());  //"thrift://192.168.40.128:9083/default/test"
            //根据配置信息从hive表中取出相应字段值
            JavaPairRDD<WritableComparable, SerializableWritable<HCatRecord>> genresRdd = hcatInputTable.read(jsc, hiveConf.getColumns());
            //转为通用格式数据
            pairRDD = genresRdd.mapToPair(new HCatToMap(hcatInputTable.getSchema(), keyLabelName));
        } else if (inputDBConf instanceof HBaseConf) {  //从hbase表中获取数据
            HBaseConf hBaseConf = (HBaseConf) inputDBConf;
            hBaseConf.check(); //检查必填值是否有空值
            HBaseInputTable inputTable = new HBaseInputTable(hBaseConf.getPath(), hBaseConf.getColumnFamily(), hBaseConf.getTable());
            JavaPairRDD<ImmutableBytesWritable, Result> rateRdd = inputTable.readSelectColumn(jsc, hBaseConf.getColumns());
            pairRDD = rateRdd.mapToPair(new ResultToMap(keyLabelName));
        }
        return pairRDD;
    }

    /**
     * 功能描述：数据写入
     *
     * @param pairRDD     数据
     * @param version     版本
     * @author qiaobin
     */
    public void write(JavaPairRDD<String, Map<String, String>> pairRDD, long version, String objName) throws Exception {
        SparkContext sparkContext = pairRDD.context();
        SparkConf conf = sparkContext.getConf();
        String hbasePath = conf.get(ConstUtil.HBASE_ROOTDIR);
        String esNodes = conf.get(ConstUtil.ES_NODES);
        String esPort = conf.get(ConstUtil.ES_PORT);
        if (!StringUtils.isNotEmpty(hbasePath)) {
            throw new MessageException("sparkConf 中必须制定hbase.rootdir");
        }
        JavaPairRDD<ImmutableBytesWritable, Put> putPairRDD;
        JavaPairRDD<String, Map<String, String>> esPairRDD;
        //保存至hbase
        HBaseOutputTable outputTable = new HBaseOutputTable(hbasePath, ConstUtil.COLUMNFAMILY_DEFAULT, objName);
        putPairRDD = pairRDD.mapToPair(new MapToPut(ConstUtil.COLUMNFAMILY_DEFAULT, version));
        outputTable.writeFromHbase(putPairRDD);

        //保存至ES
        if (StringUtils.isNotEmpty(esNodes) && StringUtils.isNotEmpty(esPort)) {
            String type = Utils.tsToDateType(version);
            esPairRDD = pairRDD.mapToPair(new MapToESMap(version));
            JavaEsSpark.saveToEsWithMeta(esPairRDD, MessageFormat.format("{0}/{1}", objName, type));
        }
    }

    /**
     * 功能描述：创建对象
     * @author qiaobin
     * @date 2017/4/25  14:33
     * @param objName 对象名称
     */
    public void createObject(String objName) {
        this.check();
        if (null != this.esClient) {
            this.esClient.createIndex(objName);
        }
        this.hBaseClient.createTable(objName, "");
    }

    /**
     * 功能描述：删除对象
     * @author qiaobin
     * @date 2017/4/25  14:33
     * @param objName 对象名称
     */
    public void deleteObject(String objName) {
        this.check();
        if (null != this.esClient) {
            this.esClient.deleteIndex(objName);
        }
        this.hBaseClient.dropTable(objName);
    }

    /**
     * 功能描述：禁用对象
     * @author qiaobin
     * @date 2017/4/25  17:00
     * @param objName 对象名称
     */
    public void disableObject(String objName) {
        this.check();
        if (null != this.esClient) {
            this.esClient.disabledIndex(objName);
        }
        this.hBaseClient.disableTable(objName);
    }

    /**
     * 功能描述：删除一条对象实体
     * @author qiaobin
     * @date 2017/4/25  14:52
     * @param objName 对象名
     * @param type
     * @param version 版本
     * @param entityId  entityId 如es有初始化此id为ES表中主键， 如没初始化该id为hbase表的rowKey
     */
    public void deleteEntity(String objName, String type, String entityId, long version) throws IOException {
        this.check();
        if (null != this.esClient) {
            if (!StringUtils.isNotEmpty(type)) {
                throw new MessageException("type 不能为空！");
            }
            ESQueryConstructor constructor = new ESQueryConstructor();
            constructor.must(new ESQueryBuilders().term(ConstUtil.VERSION, version).term(ConstUtil.ROWKEY, entityId));
            LabelResult list = this.esClient.search(objName, constructor, type);
            if (CollectionUtils.isNotEmpty(list.getEntityList())) {
                for (Entity entity : list.getEntityList()) {
                    this.esClient.deleteData(objName, type, entity.getEsId());
                }
            }
        }
        this.hBaseClient.deleteRow(objName, entityId, version);
    }

    /**
     * 功能描述：删除多条对象实体
     * @author qiaobin
     * @date 2017/4/25  14:52
     * @param objName 对象名称
     * @param type
     * @param version 版本
     * @param entityIds
     */
    public void bulkDeleteEntity(String objName, String type, Collection<String> entityIds, long version) throws IOException {
        this.check();
        if (null != this.esClient) {
            if (!StringUtils.isNotEmpty(type)) {
                throw new MessageException("type 不能为空！");
            }
            ESQueryConstructor constructor = new ESQueryConstructor();
            constructor.must(new ESQueryBuilders().term(ConstUtil.VERSION, version).terms(ConstUtil.ROWKEY, entityIds));
            LabelResult labelResult = this.esClient.search(objName, constructor, type);
            if (CollectionUtils.isNotEmpty(labelResult.getEntityList())) {
                List<String> idList = new ArrayList<>();
                for (Entity entity : labelResult.getEntityList()) {
                    idList.add(entity.getEsId());
                }
                this.esClient.bulkDeleteData(objName, type, idList);
            }
        }
        this.hBaseClient.batchDeleteRow(objName, entityIds, version);
    }

    /**
     * 功能描述：插入一条对象实体
     * @author qiaobin
     * @date 2017/4/25  15:23
     * @param objName 表名
     * @param entityId 主键（对应hbase的rowKey）
     * @param entity 数据<字段， 值>
     * @param version 版本库生成的时间戳
     */
    public void insertEntity(String objName, String entityId, Map<String, Object> entity, long version) throws Exception {
        this.check();
        this.hBaseClient.insertRow(objName, entityId, null, entity, version);
        if (null != this.esClient) {
            String type = Utils.tsToDateType(version);
            entity.put(ConstUtil.ROWKEY, entityId);
            entity.put(ConstUtil.VERSION, String.valueOf(version));
            this.esClient.insertData(objName, type, Json.toJsonString(entity));
        }
    }

    /**
     * 功能描述：批量插入数据
     * @author qiaobin
     * @date 2017/5/2  16:50
     * @param objName 对象名称
     * @param entityList  标签实体
     * @param version 版本
     */
    public void bulkInsertEntity(String objName, List<LabelEntity> entityList, long version) throws IOException {
        this.check();
        this.hBaseClient.bulkInsertRow(objName, null, entityList, version);
        if (null != this.esClient) {
            String type = Utils.tsToDateType(version);
            List<String> jsonList = new ArrayList<>();
            for (LabelEntity labelEntity : entityList) {
                Map<String, Object> entity = labelEntity.getEntity();
                entity.put(ConstUtil.ROWKEY, labelEntity.getRowKey());
                entity.put(ConstUtil.VERSION, String.valueOf(version));
                jsonList.add(Json.toJsonString(entity));
            }
            this.esClient.bulkInsertData(objName, type, jsonList);
        }
    }


    /**
     * 功能描述：ES\HBASE根据版本检索
     * @author qiaobin
     * @date 2017/4/25  17:55
     * @param objName 对象名称
     * @param constructor 查询构造
     * @param version 版本
     */
    public LabelResult search(String objName, SearchConstructor constructor, long version) throws IOException {
        this.check();
        constructor.setVersion(version);
        if (null != this.esClient) {
            String[] types = this.esClient.getSameTypes(objName, version);
            if (null == types) {
                throw new MessageException("没有可用于检索的type");
            }
            return this.esClient.search(objName, constructor, types);
        } else {
            return this.hBaseClient.search(objName, constructor, constructor.getStartEntity());
        }
    }

    /**
     * 功能描述：ES\HBASE检索
     * @author qiaobin
     * @date 2017/4/25  17:55
     * @param objName 对象名称
     * @param constructor 查询构造
     */
    public LabelResult search(String objName, SearchConstructor constructor) throws IOException {
        this.check();
        if (null != this.esClient) {
            Object[] types = this.esClient.getTypes(objName);
            if (null == types || types.length <= 0) {
                throw new MessageException("没有可用于检索的type");
            }
            String[] typesArr = new String[types.length];
            for (int i = 0; i < types.length; i++) {
                typesArr[i] = types[i].toString();
            }
            return this.esClient.search(objName, constructor, typesArr);
        } else {
            return this.hBaseClient.search(objName, constructor, constructor.getStartEntity());
        }
    }

    /**
     * 功能描述：根据实体id查询实体
     * @author qiaobin
     * @date 2017/4/25  17:53
     * @param objName 对象名
     * @param entityIds 实体ID
     * @param version 版本
     */
    public LabelResult search(String objName, Collection<String> entityIds, long version) throws IOException {
        return this.hBaseClient.getRowData(objName, entityIds, ConstUtil.COLUMNFAMILY_DEFAULT, version);
    }



    /**
     * 功能描述：根据对象获取前一天数据条数
     * @author qiaobin
     * @date 2017/4/26  10:27
     * @param objName 对象名称
     */
    public long objectTotal(String objName, long beginDate, long endDate) throws Exception{
        this.check();
        long count;
        if (null != this.esClient) {
            ESQueryConstructor constructor = new ESQueryConstructor();
            constructor.must(new ESQueryBuilders().range(ConstUtil.VERSION, beginDate, endDate));
            List<String> types = this.esClient.getTypes(objName, beginDate, endDate);
            count = this.esClient.count(objName, constructor, Utils.toArray(types));
        } else {
            count = this.hBaseClient.count(objName, beginDate, endDate);
        }
        return count;
    }

    /**
     * 功能描述：标签数据覆盖率,  标签不为空数/总数
     * @author qiaobin
     * @date 2017/4/26  10:25
     * @param objName 对象名称
     * @param labelName 标签名称
     */
    public String labelCoverageRate(String objName, String labelName) throws Exception {
        double percent = 0;
        long notNullCount = 0;
        long totalCount = 0;
        DecimalFormat df = new DecimalFormat("0.00");
        if (null != this.esClient) {
            ESQueryConstructor constructor = new ESQueryConstructor();
            constructor.must(new ESQueryBuilders().exists(labelName));
            notNullCount = this.esClient.count(objName, constructor, Utils.toStringArray(this.esClient.getTypes(objName)));
            totalCount = this.esClient.getTotalCount(objName, Utils.toStringArray(this.esClient.getTypes(objName)));
        } else {
            notNullCount = this.hBaseClient.count(objName, labelName);
            totalCount = this.hBaseClient.count(objName, 0, 0);
        }
        percent = Double.parseDouble(notNullCount+"") / Double.parseDouble(totalCount+"") * 100;
        return df.format(percent);
    }

}



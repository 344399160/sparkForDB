package com.scistor.label.elasticsearch;

import com.scistor.label.common.MessageException;
import com.scistor.label.utils.ConstUtil;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 描述：ES检索
 * author qiaobin   2017/3/6 14:20.
 */
public class ESService {

    private final static int MAX = 100;

    private final static String SIMPLE_FORMATTER = "yyyyMMdd";

    private TransportClient client;

    /**
     * 功能描述：服务初始化
     * @param clusterName 集群名称
     * @param ip 地址
     * @param port 端口
     */
    public ESService(String clusterName, String ip, int port) {
        try {
            Settings settings = Settings.builder()
                    .put("cluster.name", clusterName).build();
            this.client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ip), port));
        } catch (UnknownHostException e) {
            throw new MessageException("es init failed!", e);
        }
    }

    /**
     * 功能描述：新建索引
     * @param indexName 索引名
     */
    public void createIndex(String indexName) {
        if (this.indexExist(indexName) == false)
            client.admin().indices().create(new CreateIndexRequest(indexName.toLowerCase()))
                .actionGet();
    }

    /**
     * 功能描述：删除索引
     * @param index 索引名
     */
    public void deleteIndex(String index) {
        if (indexExist(index)) {
            DeleteIndexResponse dResponse = client.admin().indices().prepareDelete(index.toLowerCase())
                    .execute().actionGet();
            if (!dResponse.isAcknowledged()) {
                throw new MessageException("failed to delete index.");
            }
        }
    }

    /**
     * 功能描述：关闭索引
     * @param index 索引名
     */
    public void disabledIndex(String index) {
        if (indexExist(index)) {
            CloseIndexResponse dResponse = client.admin().indices().prepareClose(index)
                    .execute().actionGet();
            if (!dResponse.isAcknowledged()) {
                throw new MessageException("failed to disable index.");
            }
        } else {
            throw new MessageException("index name not exists");
        }
    }

    /**
     * 功能描述：验证索引是否存在
     * @param index 索引名
     */
    public boolean indexExist(String index) {
        IndicesExistsRequest inExistsRequest = new IndicesExistsRequest(index);
        IndicesExistsResponse inExistsResponse = client.admin().indices()
                .exists(inExistsRequest).actionGet();
        return inExistsResponse.isExists();
    }

    /**
     * 功能描述：插入数据
     * @param index 索引名
     * @param type 类型
     * @param dataJson 数据
     */
    public void insertData(String index, String type, String dataJson) {
        client.prepareIndex(index, type).setSource(dataJson).get();
    }

    /**
     * 功能描述：插入数据
     * @param index 索引名
     * @param type 类型
     * @param _id 数据id
     * @param dataJson 数据
     */
    public void insertData(String index, String type, String _id, String dataJson) {
        client.prepareIndex(index, type).setId(_id).setSource(dataJson).get();
    }

    /**
     * 功能描述：更新数据
     * @param index 索引名
     * @param type 类型
     * @param _id 数据id
     * @param dataJson 数据
     */
    public void updateData(String index, String type, String _id, String dataJson) throws Exception {
        try {
            UpdateRequest updateRequest = new UpdateRequest(index, type, _id)
                    .doc(dataJson);
            client.update(updateRequest).get();
        } catch (Exception e) {
            throw new MessageException("update data failed.", e);
        }
    }

    /**
     * 功能描述：删除数据
     * @param index 索引名
     * @param type 类型
     * @param _id 数据id
     */
    public void deleteData(String index, String type, String _id) {
        client.prepareDelete(index, type, _id).get();
    }

    /**
     * 功能描述：批量插入数据
     * @param index 索引名
     * @param type 类型
     * @param dataJsons 批量数据
     */
    public void bulkInsertData(String index, String type, List<String> dataJsons) {
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for (String item : dataJsons) {
            bulkRequest.add(client.prepareIndex(index, type)
                    .setSource(item)
            );
        }
        bulkRequest.get();
    }

    /**
     * 功能描述：批量删除数据
     * @param index 索引名
     * @param type 类型
     * @param idList 主键
     */
    public void bulkDeleteData(String index, String type, Collection<String> idList) {
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for (String _id : idList) {
            bulkRequest.add(client.prepareDelete(index, type, _id)
            );
        }
        bulkRequest.get();
    }

    /**
     * 功能描述：查询
     * @param index 索引名
     * @param type 类型
     * @param constructor 查询构造
     */
    public List<Map<String, Object>> search(String index, ESQueryBuilderConstructor constructor, String ... type) {
        List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
        String[] a = {};
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setTypes(type);
        //排序
        if (StringUtils.isNotEmpty(constructor.getAsc()))
            searchRequestBuilder.addSort(constructor.getAsc(), SortOrder.ASC);
        if (StringUtils.isNotEmpty(constructor.getDesc()))
            searchRequestBuilder.addSort(constructor.getDesc(), SortOrder.DESC);
        //设置查询体
        searchRequestBuilder.setQuery(constructor.listBuilders());
        //返回条目数
        int size = constructor.getSize();
        if (size < 0) {
            size = 0;
        }
        if (size > MAX) {
            size = MAX;
        }
        //返回条目数
        searchRequestBuilder.setSize(size);

        searchRequestBuilder.setFrom(constructor.getFrom() < 0 ? 0 : constructor.getFrom());

        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();

        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHists = hits.getHits();
        for (SearchHit sh : searchHists) {
            Map<String, Object> map = new HashMap<String, Object>();
            map.put("_id", sh.getId());
            map.putAll(sh.getSource());
            result.add(map);
        }
        return result;
    }

    /**
     * 功能描述：统计查询
     * @param index 索引名
     * @param type 类型
     * @param constructor 查询构造
     * @param statBy 统计字段
     */
    public Map<Object, Object> statSearch(ESQueryBuilderConstructor constructor, String statBy, String index, String... type) {
        Map<Object, Object> map = new HashedMap();
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setTypes(type);
        //设置查询体
        if (null != constructor) {
            searchRequestBuilder.setQuery(constructor.listBuilders());
        } else {
            constructor = new ESQueryBuilderConstructor();
            searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery());
        }
        int size = constructor.getSize();
        if (size < 0) {
            size = 0;
        }
        if (size > MAX) {
            size = MAX;
        }
        //返回条目数
        searchRequestBuilder.setSize(size);

        SearchResponse sr = searchRequestBuilder.addAggregation(
                AggregationBuilders.terms("agg").field(statBy)
        ).get();

        Terms stateAgg = sr.getAggregations().get("agg");
        Iterator<Terms.Bucket> iter = stateAgg.getBuckets().iterator();

        while (iter.hasNext()) {
            Terms.Bucket gradeBucket = iter.next();
            map.put(gradeBucket.getKey(), gradeBucket.getDocCount());
        }

        return map;
    }

    /**
     * 功能描述：统计查询
     * @param index 索引名
     * @param type 类型
     * @param constructor 查询构造
     * @param agg 自定义计算
     */
    public Map<Object, Object> statSearch(ESQueryBuilderConstructor constructor, AggregationBuilder agg, String index,  String type) {
        if (agg == null) {
            return null;
        }
        Map<Object, Object> map = new HashedMap();
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setTypes(type);
        //设置查询体
        if (null != constructor) {
            searchRequestBuilder.setQuery(constructor.listBuilders());
        } else {
            constructor = new ESQueryBuilderConstructor();
            searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery());
        }
        int size = constructor.getSize();
        if (size < 0) size = 0;
        if (size > MAX) size = MAX;
        //返回条目数
        searchRequestBuilder.setSize(size);

        SearchResponse sr = searchRequestBuilder.addAggregation(agg).get();

        Terms stateAgg = sr.getAggregations().get("agg");
        Iterator<Terms.Bucket> iter = stateAgg.getBuckets().iterator();

        while (iter.hasNext()) {
            Terms.Bucket gradeBucket = iter.next();
            map.put(gradeBucket.getKey(), gradeBucket.getDocCount());
        }
        return map;
    }

    /**
     * 功能描述：获取符合时间范围的types
     * @author qiaobin
     * @date 2017/4/5  16:55
     * @param index 索引
     * @param beforeTime 起始时间
     * @param endTime 结束时间
     */
    public List<String> getTypes(String index, long beforeTime, long endTime) {
        List<String> list = new ArrayList<String>();
        try {
            Object[] keys = getTypes(index);
            SimpleDateFormat sdf = new SimpleDateFormat(SIMPLE_FORMATTER);
            for (Object key : keys) {
                Date typeDate = sdf.parse(key.toString().split(ConstUtil.ES_TYPE_REGEX)[0]);
                if (typeDate.getTime() > beforeTime && typeDate.getTime() < endTime)
                    list.add(key.toString());
            }
        } catch (Exception e) {
            throw new MessageException("获取类型失败", e);
        }
        return list;
    }

    /**
     * 功能描述：找出索引下同时间戳的所有type
     * 如 timestamp 的时间戳转换为 19970101 那么会找出索引下匹配该事件的所有type，如：19970101-1,19970101-2，然后将最大数+1返回，该例返回19970101-3， 如没有将返回19970101-1
     * @author qiaobin
     * @date 2017/4/15  17:14
     * @param index 索引
     * @param timestamp 版本时间戳
     */
    public String generateType(String index, long timestamp) throws Exception{
        SimpleDateFormat sdf = new SimpleDateFormat(SIMPLE_FORMATTER);
        String genType = "";
        Object[] types = getTypes(index);
        Timestamp ts = new Timestamp(timestamp);
        String typePrefix = sdf.format(ts);
        //找出和该timestamp为同一天的所有type
        List<String> samePrefixType = new ArrayList<>();
        int count = 0;
        for (Object type : types) {
            if (type.toString().split(ConstUtil.ES_TYPE_REGEX)[0].equals(typePrefix)) {
                samePrefixType.add(type.toString());
            }
        }
        //如果没有该类型为其初始化一个type, 如果有找出最后一个type,并将-后的数字 +1
        if (count == 0) {
            genType = typePrefix + ConstUtil.ES_TYPE_REGEX + 1;
        } else {
            String preType = samePrefixType.get(0).split(ConstUtil.ES_TYPE_REGEX)[0];
            int max = Integer.parseInt(samePrefixType.get(0).split(ConstUtil.ES_TYPE_REGEX)[1]);
            if (samePrefixType.size() != 1) { //不止一个匹配type
                for (int i = 1; i < samePrefixType.size(); i++) {
                    int count1 = Integer.parseInt(samePrefixType.get(i).split(ConstUtil.ES_TYPE_REGEX)[1]);
                    if (count1 > max)
                        max = count1;
                }
            }
            long typeTotalCount = getTotalCount(index, preType + ConstUtil.ES_TYPE_REGEX + max);   //检查最后一个type存储数量大于阈值，如果超出新建新type, 否则返回该type
            if (typeTotalCount > ConstUtil.ES_TYPE_EXCEED_NUM) {
                genType = preType + ConstUtil.ES_TYPE_REGEX + (max + 1);
            } else {
                genType = preType + ConstUtil.ES_TYPE_REGEX + max;
            }
        }
        return genType;
    }

    /**
     * 功能描述：索引下type
     * @author qiaobin
     * @date 2017/4/6  15:46
     * @param index
     */
    public Object[] getTypes(String index) {
        IndicesAdminClient indicesAdminClient = client.admin().indices();
        GetMappingsRequest gmr = indicesAdminClient.prepareGetMappings(index).request();
        ActionFuture<GetMappingsResponse> mapping = indicesAdminClient.getMappings(gmr);
        GetMappingsResponse response1 = mapping.actionGet();
        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> map = response1.getMappings();
        Object[] keys = map.get(index).keys().toArray();
        return keys;
    }

    /**
     * 功能描述：索引总数据数
     * @author qiaobin
     * @date 2017/4/6  15:19
     * @param index
     */
    public long getTotalCount(String index) {
        IndicesStatsResponse isr = client.admin().indices().prepareStats(index).get();
        long total = isr.getTotal().getDocs().getCount();
        return total;
    }

    /**
     * 功能描述：索引下类型总数据数
     * @author qiaobin
     * @date 2017/4/6  15:19
     * @param index
     * @param type
     */
    public long getTotalCount(String index, String type) {
        SearchResponse sp = client.prepareSearch(index).setTypes(type).setSize(0).execute().actionGet();
        return sp.getHits().getTotalHits();
    }

    /**
     * 功能描述：关闭链接
     */
    public void close() {
        client.close();
    }


}

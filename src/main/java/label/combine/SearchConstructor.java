package label.combine;

import label.utils.ConstUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.ArrayList;
import java.util.List;

/**
 * 描述：构造器
 * author qiaobin   2017/4/26 15:17.
 */
public class SearchConstructor {

    //版本
    private long version;

    //起始条数
    private int from;

    //查询条数
    private int size;

    //其实查询rowkey
    private String startEntity;

    //查询条件容器
    private List<Criterion> criterions = new ArrayList<>();

    //构造builder
    public QueryBuilder listBuilders() {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        QueryBuilder queryBuilder = null;
        if (!CollectionUtils.isEmpty(criterions)) {
            for (Criterion criterion : criterions) {
                for (QueryBuilder builder : criterion.listBuilders()) {
                    queryBuilder = boolQueryBuilder.must(builder);
                }
            }
        }
        if (version != 0) {
            queryBuilder = boolQueryBuilder.must(QueryBuilders.termQuery(ConstUtil.VERSION, version));
        }
        return queryBuilder;
    }

    //构造FilterList
    public FilterList listFilters() {
        FilterList filterList = new FilterList();
        if (!CollectionUtils.isEmpty(criterions)) {
            for (Criterion criterion : criterions) {
                for (Filter filter : criterion.listFilters().getFilters()) {
                    filterList.addFilter(filter);
                }
            }
        }
        return filterList;
    }

    public SearchConstructor add(Criterion criterion) {
        if (null != criterion) {
            this.criterions.add(criterion);
        }
        return this;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public int getFrom() {
        return from;
    }

    public void setFrom(int from) {
        this.from = from;
    }

    public int getSize() {
        if (size == 0) {
            size = ConstUtil.QUERY_SIZE;
        }
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public String getStartEntity() {
        return startEntity;
    }

    public void setStartEntity(String startEntity) {
        this.startEntity = startEntity;
    }
}

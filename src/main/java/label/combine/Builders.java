package label.combine;

import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.ArrayList;
import java.util.List;


/**
 * 条件构造器 
 * 用于创建条件表达式 
 */  
public class Builders implements Criterion{

    private List<QueryBuilder> queryBuilders = new ArrayList<>();
    private List<Filter> filters = new ArrayList<>();

    /**
     * 功能描述：Term 查询
     * @param field 字段名
     * @param value 值
     */
    public Builders equal(String field, Object value) {
        queryBuilders.add(new SimpleExpression (field, value, Operator.TERM).toBuilder());
        filters.add(new SimpleExpression(field, value, Operator.TERM).toFilter());
        return this;
    }

    /**
     * 功能描述：fuzzy 查询
     * @param field 字段名
     * @param value 值
     */
    public Builders like(String field, Object value) {
        queryBuilders.add(new SimpleExpression (field, value, Operator.FUZZY).toBuilder());
        filters.add(new SimpleExpression(field, value, Operator.FUZZY).toFilter());
        return this;
    }

    /**
     * 功能描述：Range 查询
     * @param from 起始值
     * @param to 末尾值
     */
    public Builders range(String field, Object from, Object to) {
        queryBuilders.add(new SimpleExpression (field, from, to, Operator.RANGE).toBuilder());
        filters.add(new SimpleExpression(field, from, Operator.LESS).toFilter());
        filters.add(new SimpleExpression(field, from, Operator.GREATER_EQUAL).toFilter());
        return this;
    }

    /**
     * 功能描述：非空查询
     * @param field 校验字段
     */
    public Builders exists(String field) {
        queryBuilders.add(new SimpleExpression(field).toBuilder());
        return this;
    }


    public List<QueryBuilder> listBuilders() {
        return queryBuilders;
    }

    public FilterList listFilters() {
        FilterList filterList = new FilterList();
        for (Filter filter : filters) {
            filterList.addFilter(filter);
        }
        return filterList;
    }

}

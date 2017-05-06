package label.combine;

import label.elasticsearch.ESCriterion;
import label.utils.ConstUtil;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.Collection;


/**
 * 简单条件表达式 
 */
public class SimpleExpression {
	private String fieldName;       //属性名
    private Object value;           //对应值
    private Collection<Object> values;           //对应值
    private Criterion.Operator operator;      //计算符
    private Object from;
    private Object to;

    protected SimpleExpression(String fieldName) {
        this.fieldName = fieldName;
    }

    protected SimpleExpression(String fieldName, Object value, Criterion.Operator operator) {
        this.fieldName = fieldName;
        this.value = value;
        this.operator = operator;
    }

    protected SimpleExpression(String value, Criterion.Operator operator) {
        this.value = value;
        this.operator = operator;
    }

    protected SimpleExpression(String fieldName, Collection<Object> values) {
        this.fieldName = fieldName;
        this.values = values;
        this.operator = Criterion.Operator.TERMS;
    }

    protected SimpleExpression(String fieldName, Object from, Object to, Criterion.Operator operator) {
        this.fieldName = fieldName;
        this.from = from;
        this.to = to;
        this.operator = operator;
    }

    public QueryBuilder toBuilder() {
        QueryBuilder qb = null;
        switch (operator) {
            case TERM:
                qb = QueryBuilders.termQuery(fieldName, value);
                break;
            case TERMS:
                qb = QueryBuilders.termsQuery(fieldName, values);
                break;
            case RANGE:
                qb = QueryBuilders.rangeQuery(fieldName).from(from).to(to).includeLower(true).includeUpper(true);
                break;
            case FUZZY:
                qb = QueryBuilders.fuzzyQuery(fieldName, value);
                break;
            case QUERY_STRING:
                qb = QueryBuilders.queryStringQuery(value.toString());
                break;
            case EXSISTS:
                qb = QueryBuilders.existsQuery(fieldName);
        }
        return qb;
    }

    public Filter toFilter() {
        Filter filter = null;
        switch (operator) {
            case TERM:
                filter = new SingleColumnValueFilter(Bytes
                    .toBytes(ConstUtil.COLUMNFAMILY_DEFAULT), Bytes.toBytes(fieldName), CompareFilter.CompareOp.EQUAL, Bytes
                    .toBytes(value.toString()));
                break;
            case GREATER_EQUAL:
                filter = new SingleColumnValueFilter(Bytes
                        .toBytes(ConstUtil.COLUMNFAMILY_DEFAULT), Bytes.toBytes(fieldName), CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes
                        .toBytes(value.toString()));
                break;
            case LESS:
                filter = new SingleColumnValueFilter(Bytes
                        .toBytes(ConstUtil.COLUMNFAMILY_DEFAULT), Bytes.toBytes(fieldName), CompareFilter.CompareOp.LESS, Bytes
                        .toBytes(value.toString()));
                break;
            case FUZZY:
                ByteArrayComparable comparator = new RegexStringComparator(value.toString());
                filter = new SingleColumnValueFilter(Bytes
                        .toBytes(ConstUtil.COLUMNFAMILY_DEFAULT), Bytes.toBytes(fieldName), CompareFilter.CompareOp.EQUAL, comparator);
        }
        return filter;
    }

}

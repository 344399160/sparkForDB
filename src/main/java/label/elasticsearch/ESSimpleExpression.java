package label.elasticsearch;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.Collection;


/**
 * 简单条件表达式 
 */
public class ESSimpleExpression {
	private String fieldName;       //属性名
    private Object value;           //对应值
    private Collection<String> values;           //对应值
    private ESCriterion.Operator operator;      //计算符
    private Object from;
    private Object to;

    protected  ESSimpleExpression(String fieldName, Object value, ESCriterion.Operator operator) {
        this.fieldName = fieldName;
        this.value = value;
        this.operator = operator;
    }

    protected  ESSimpleExpression(String value, ESCriterion.Operator operator) {
        this.value = value;
        this.operator = operator;
    }

    protected ESSimpleExpression(String fieldName, Collection<String> values) {
        this.fieldName = fieldName;
        this.values = values;
        this.operator = ESCriterion.Operator.TERMS;
    }

    protected ESSimpleExpression(String fieldName, Object from, Object to, ESCriterion.Operator operator) {
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
                qb = QueryBuilders.existsQuery(value.toString());
        }
        return qb;
    }

}

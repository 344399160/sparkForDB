package label.combine;

import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.List;

/**
 * 条件接口 
 * 用户提供条件表达式接口 
 */  
public interface Criterion {
	public enum Operator {  
        TERM, TERMS, RANGE, FUZZY, QUERY_STRING, MISSING, LESS, GREATER_EQUAL, EXSISTS
    }
	
	public enum MatchMode {  
		START, END, ANYWHERE
	}  
	
	public enum Projection {
		MAX, MIN, AVG, LENGTH, SUM, COUNT
	}

	public List<QueryBuilder> listBuilders();

	public FilterList listFilters();
}

package label.elasticsearch;

import org.elasticsearch.index.query.QueryBuilder;

import java.util.List;

/**
 * 条件接口 
 * 用户提供条件表达式接口 
 */  
public interface ESCriterion {
	public enum Operator {  
        TERM, TERMS, RANGE, FUZZY, QUERY_STRING, MISSING
    }
	
	public enum MatchMode {  
		START, END, ANYWHERE
	}  
	
	public enum Projection {
		MAX, MIN, AVG, LENGTH, SUM, COUNT
	}

	public List<QueryBuilder> listBuilders();
}

package com.scistor.label.elasticsearch;

import org.apache.commons.collections.CollectionUtils;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.ArrayList;
import java.util.List;

/**
 * 定义一个查询条件容器 
 */
public class ESQueryBuilderConstructor {

	private int size = 100;

	private int from = 0;

	private String asc;

	private String desc;

	//查询条件容器
	private List<ESCriterion> mustCriterions = new ArrayList<ESCriterion>();
	private List<ESCriterion> shouldCriterions = new ArrayList<ESCriterion>();
	private List<ESCriterion> mustNotCriterions = new ArrayList<ESCriterion>();

	//构造builder
    public QueryBuilder listBuilders() {
		int count = mustCriterions.size() + shouldCriterions.size() + mustNotCriterions.size();
		BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
		QueryBuilder queryBuilder = null;

		if (count >= 1) {
			//must容器
			if (!CollectionUtils.isEmpty(mustCriterions)) {
				for (ESCriterion criterion : mustCriterions) {
					for (QueryBuilder builder : criterion.listBuilders()) {
						queryBuilder = boolQueryBuilder.must(builder);
					}
				}
			}
			//should容器
			if (!CollectionUtils.isEmpty(shouldCriterions)) {
				for (ESCriterion criterion : shouldCriterions) {
					for (QueryBuilder builder : criterion.listBuilders()) {
						queryBuilder = boolQueryBuilder.should(builder);
					}

				}
			}
			//must not 容器
			if (!CollectionUtils.isEmpty(mustNotCriterions)) {
				for (ESCriterion criterion : mustNotCriterions) {
					for (QueryBuilder builder : criterion.listBuilders()) {
						queryBuilder = boolQueryBuilder.mustNot(builder);
					}
				}
			}
			return queryBuilder;
		} else {
			return null;
		}
	}

    /** 
     * 增加简单条件表达式 
     */
    public ESQueryBuilderConstructor must(ESCriterion criterion){
        if(criterion!=null){
			mustCriterions.add(criterion);
		}
		return this;
	}
    /**
     * 增加简单条件表达式
     */
    public ESQueryBuilderConstructor should(ESCriterion criterion){
        if(criterion!=null){
			shouldCriterions.add(criterion);
		}
		return this;
	}
    /**
     * 增加简单条件表达式
     */
    public ESQueryBuilderConstructor mustNot(ESCriterion criterion){
        if(criterion!=null){
			mustNotCriterions.add(criterion);
		}
		return this;
	}


	public int getSize() {
		return size;
	}

	public void setSize(int size) {
		this.size = size;
	}

	public String getAsc() {
		return asc;
	}

	public void setAsc(String asc) {
		this.asc = asc;
	}

	public String getDesc() {
		return desc;
	}

	public void setDesc(String desc) {
		this.desc = desc;
	}

	public int getFrom() {
		return from;
	}

	public void setFrom(int from) {
		this.from = from;
	}
}

package com.itwolf.solrj;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Test;

/**
 * 通过solr的java接口访问 Solr服务器
 * 完成solr服务器的管理
 * @author wolf
 *
 */
public class SolrJDemo {
	@Test //id相同就是修改,id不同就是添加
	public void testAdd() throws SolrServerException, IOException{
		String baseURL = "http://localhost:8080/solr";//默认访问collection1
		//也可以指定
		//string baseURL = "http://localhost:8080/solr/collection1"
		//1.连接solr服务器
		SolrServer solrServer = new HttpSolrServer(baseURL);
		//添加 
		//2.创建 SolrInputDocument对象
		SolrInputDocument doc = new SolrInputDocument();
		doc.setField("id", 111);
		doc.setField("name", "jacccccc");
		//3. 把SolrInputDocument对象添加到索引库中
			//第二个参数是 多少毫秒后自动提交事务
		solrServer.add(doc, 1000);
	}
	//删除
	@Test
	public void testDelete() throws SolrServerException, IOException{
		String baseURL = "http://localhost:8080/solr";//默认访问collection1
		//也可以指定
		//string baseURL = "http://localhost:8080/solr/collection1"
		//1.连接solr服务器
		SolrServer solrServer = new HttpSolrServer(baseURL);
		//2.删除逻辑有两种:
		//				1.根据id删除
		//第二个参数是 多少毫秒后自动提交事务
		solrServer.deleteById("110",1000);
		//				2.根据条件删除      *:* 是全删除  (慎用)  name:jack
		//solrServer.deleteByQuery("*:*", 1000);
	}
	
	//查询
	@Test
	public void testQuery() throws SolrServerException{
		String baseURL = "http://localhost:8080/solr";//默认访问collection1
		//也可以指定
		//string baseURL = "http://localhost:8080/solr/collection1"
		//1.连接solr服务器
		SolrServer solrServer = new HttpSolrServer(baseURL);
		//2.创建搜索对象
		SolrQuery params = new SolrQuery();
		//3.设置搜索条件
		params.setQuery("*:*");
		//4.发起搜索请求
		QueryResponse response = solrServer.query(params);
		//5.处理搜索结果
		SolrDocumentList results = response.getResults();
		//打印输出搜索到多少条记录
		System.out.println("搜索到结果总数:"+results.getNumFound());
		
		//遍历搜索结果
		for (SolrDocument solrDocument : results) {
			System.out.println("id:"+solrDocument.get("id"));
			System.out.println("name:"+solrDocument.get("name"));
		}
	}
	
	//solrj的复杂查询
	@Test
	public void querySolrj() throws SolrServerException{
		String baseURL = "http://localhost:8080/solr";//默认访问collection1
		//也可以指定
		//string baseURL = "http://localhost:8080/solr/collection1"
		//1.连接solr服务器
		SolrServer solrServer = new HttpSolrServer(baseURL);
		//创建一个query对象
		SolrQuery query = new SolrQuery();
		//1.设置查询条件
		query.setQuery("钻石");
		//2.设置过滤条件
		query.setFilterQueries("product_price:[5 TO 10]","product_catalog_name:品味茶杯");
		//或者这样写
		//query.set("fq", "product_price:[5 TO 10]");
		//query.set("fq", "product_price:[5 TO 10],product_catalog_name:品味茶杯");
		//还可以这样写
		//query.addFilterQuery("product_price:[5 TO 10]","product_catalog_name:品味茶杯");
		//3.排序
		query.setSort("product_price", ORDER.desc);
		//4.分页处理
		query.setStart(0);
		query.setRows(10);
		//5.结果中域的列表  查询指定的域名
		query.setFields("id","product_name","product_price","product_catalog_name","product_picture");
		//6.默认查询的域
		query.set("df", "product_keywords");
		//7.高亮
		//	7.1开启高亮
		query.setHighlight(true);
		//	7.2设置高亮的域
		query.addHighlightField("product_name");
		//	7.3高亮前缀
		query.setHighlightSimplePre("<span style='color:red'>");
		//	7.4高亮后缀
		query.setHighlightSimplePost("</span>");
		
		//执行查询
		QueryResponse response = solrServer.query(query);
		//获取高亮
		Map<String, Map<String, List<String>>> highlighting = response.getHighlighting();
		//获取查询结果集
		SolrDocumentList docs = response.getResults();
		//获取总条数
		long numFound = docs.getNumFound();
		System.out.println("查询结果的总条数:"+numFound);
		for (SolrDocument doc : docs) {
			//获取id id是唯一的
			String id = (String) doc.get("id");
			Map<String, List<String>> map = highlighting.get(id);
			List<String> list = map.get("product_name");
			//设置高亮显示的名
			String highLightName = "";
			//判断是否有高亮内容
			if (null != list) {
				highLightName = list.get(0);
			} else {
				highLightName = (String) doc.get("product_name");
			}
			System.out.println("高亮的名称:"+highLightName);
			System.out.println("product_price:"+doc.get("product_price"));
			System.out.println("product_catalog_name:"+doc.get("product_catalog_name"));
			System.out.println("product_picture:"+doc.get("product_picture"));
		}
	}
}

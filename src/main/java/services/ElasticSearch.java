package services;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import models.OpResult;
import models.ResultDocument;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.SearchHit;

public class ElasticSearch {
	private static Log log = LogFactory.getLog(ElasticSearch.class);

	private static ElasticSearch es;

	public static void BuildElasticSearch() {
		es = new ElasticSearch("test_index", "test_cluster", "test_type");
	}

	public static ElasticSearch getElasticSearch() throws InterruptedException {
		while (es == null)
			Thread.sleep(1000);
		return es;
	}

	private Node elasticNode;
	private String indexName;
	private String clusterName;
	private String typeName;

	private ElasticSearch(String indexName, String clusterName, String typeName) {
		this.indexName = indexName;
		this.typeName = typeName;
		this.clusterName = clusterName;
		this.elasticNode = NodeBuilder.nodeBuilder()
				.clusterName(this.clusterName).node();

		// Create only Once Document Index
		try {
			this.deleteIndex();
		} catch (Exception e) {
			log.debug("Couldnt delete index");
		}
		this.createIndex();
		this.createMapping();
		this.createExamples();

	}

	public OpResult addDocument(String doc) {
		try {
			IndexRequestBuilder indexRequestBuilder = this.elasticNode.client()
					.prepareIndex(this.indexName, this.typeName);

			XContentBuilder contentBuilder = XContentFactory.jsonBuilder()
					.startObject().field("document", doc).endObject();
			indexRequestBuilder.setSource(contentBuilder);

			indexRequestBuilder.execute().actionGet();
		} catch (Exception e) {
			log.error(e.getMessage());
			return new OpResult(400, e.getMessage());
		}

		return new OpResult(200, "ok");
	}

	public List<ResultDocument> find(String docPattern) {
		List<ResultDocument> res = new ArrayList<ResultDocument>();

		try {

			QueryBuilder qb = QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("document", docPattern));

			log.debug("Querying...: " + qb.toString());

			SearchRequestBuilder srb = this.elasticNode.client()
					.prepareSearch(this.indexName)
					.setSearchType(SearchType.QUERY_AND_FETCH).setQuery(qb)
					.setFrom(0).setSize(60);

			SearchResponse response = srb.execute().actionGet();

			Iterator<SearchHit> it = response.getHits().iterator();

			while (it.hasNext()) {
				log.debug("Adding result");
				res.add(new ResultDocument(it.next().getSource()
						.get("document").toString()));
			}

		} catch (SearchPhaseExecutionException e) {

			log.error(e.getMessage());
			log.error(e.getDetailedMessage());
		}
		return res;
	}

	// Index Setup
	private void createIndex() {
		CreateIndexRequestBuilder createIndexRequestBuilder = this.elasticNode
				.client().admin().indices().prepareCreate(this.indexName);
		createIndexRequestBuilder.execute().actionGet();
	}

	private void deleteIndex() {
		this.elasticNode.client().admin().indices()
				.delete(new DeleteIndexRequest(this.indexName)).actionGet();

	}

	private void createMapping() {
		try {
			PutMappingResponse putMappingResponse = this.elasticNode
					.client()
					.admin()
					.indices()
					.preparePutMapping(this.indexName)
					.setType(this.typeName)
					.setSource(
							XContentFactory.jsonBuilder().prettyPrint()
									.startObject()
											.startObject("properties")
												.startObject("document")
													.field("type", "string")
													.field("analyzer", "standard")
												.endObject()
											.endObject()
										.endObject()
								).execute().actionGet();

		} catch (ElasticsearchException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void createExamples(){
		this.addDocument("Messi es el mejor!");
		this.addDocument("Messi es bastante bueno...");
		this.addDocument("El asado es lo mejor!");
	}

}

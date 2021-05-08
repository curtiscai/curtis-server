package com.curtis.elasticsearch.client;

import com.curtis.elasticsearch.model.User;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

@SpringBootTest
@RunWith(SpringRunner.class)
public class DocumentTest {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    private static final Logger LOGGER = LoggerFactory.getLogger(DocumentTest.class);

    @Test
    public void testCreateDoc() {

        User user = new User("curtis1", null, true, BigDecimal.valueOf(181.1));

        IndexRequest indexRequest = new IndexRequest("idx_test_1");
        indexRequest.id("1");
        try {
            indexRequest.source(new ObjectMapper().writeValueAsString(user), XContentType.JSON);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        try {
            IndexResponse indexResponse = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
            // the response is : {"shardInfo":{"total":2,"successful":1,"failures":[],"failed":0,"fragment":false},"shardId":{"index":{"name":"idx_test_1","uuid":"_na_","fragment":false},"id":-1,"indexName":"idx_test_1","fragment":true},"id":"1","type":"_doc","version":1,"seqNo":0,"primaryTerm":1,"result":"CREATED","index":"idx_test_1","fragment":false}
            LOGGER.info("the response is : {}", new ObjectMapper().writeValueAsString(indexResponse));

            // the response is : CREATED
            LOGGER.info("the response is : {}", indexResponse.status());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testIsExists() {
        GetRequest getRequest = new GetRequest("idx_test_1", "1");

        boolean exists = false;
        try {
            exists = restHighLevelClient.exists(getRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // the response is : true
        LOGGER.info("the response is : {}", exists);
    }

    @Test
    public void testGetDoc() {
        GetRequest indexRequest = new GetRequest("idx_test_1", "1");
        GetResponse getResponse = null;
        try {
            getResponse = restHighLevelClient.get(indexRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // the response is : {"_index":"idx_test_1","_type":"_doc","_id":"1","_version":1,"_seq_no":0,"_primary_term":1,"found":true,"_source":{"name":"curtis1","birth":null,"sex":true,"height":181.1}}
        LOGGER.info("the response is : {}", getResponse);
    }

    @Test
    public void testUpdateDoc() {

        User user = new User("curtis2", null, true, BigDecimal.valueOf(181.2));

        UpdateRequest updateRequest = new UpdateRequest("idx_test_1", "1");
        try {
            updateRequest.doc(new ObjectMapper().writeValueAsString(user), XContentType.JSON);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        UpdateResponse updateResponse = null;
        try {
            updateResponse = restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // the response is : UpdateResponse[index=idx_test_1,type=_doc,id=1,version=2,seqNo=1,primaryTerm=1,result=updated,shards=ShardInfo{total=2, successful=1, failures=[]}]
        LOGGER.info("the response is : {}", updateResponse);
        // the response is : OK
        LOGGER.info("the response is : {}", updateResponse.status());
    }

    @Test
    public void testDeleteDoc() {
        DeleteRequest deleteRequest = new DeleteRequest("idx_test_1", "1");
        DeleteResponse deleteResponse = null;
        try {
            deleteResponse = restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // the response is : DeleteResponse[index=idx_test_1,type=_doc,id=1,version=3,result=deleted,shards=ShardInfo{total=2, successful=1, failures=[]}]
        LOGGER.info("the response is : {}", deleteResponse);
        // the response is : OK
        LOGGER.info("the response is : {}", deleteResponse.status());
    }


    /**
     * 批量操作：批量创建、更新、删除
     *
     * @throws JsonProcessingException
     */
    @Test
    public void testCreateBatchDoc() throws JsonProcessingException {
        List<User> userList = Lists.newArrayList();
        for (int i = 1; i <= 10000; i++) {
            userList.add(new User("curtis" + i, null, i % 2 == 0, BigDecimal.valueOf(181.0 + (i % 10))));
        }

        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 1; i <= 10000; i++) {
            IndexRequest indexRequest = new IndexRequest("idx_test_1");
            indexRequest.id(String.valueOf(i));
            indexRequest.source(new ObjectMapper().writeValueAsString(userList.get(i - 1)), XContentType.JSON);
            bulkRequest.add(indexRequest);
        }

        BulkResponse bulkResponse = null;
        try {
            bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // LOGGER.info("the response is : {}", new ObjectMapper().writeValueAsString(bulkResponse.));
        // the response is : OK
        LOGGER.info("the response is : {}", bulkResponse.status());
        // the response is : false
        LOGGER.info("the response is : {}", bulkResponse.hasFailures());
        // the response is : 287ms
        LOGGER.info("the response is : {}", bulkResponse.getTook());
    }

    @Test
    public void testSerchDoc() throws JsonProcessingException {


        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // 精确
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "curtis1");
        searchSourceBuilder.query(termQueryBuilder);

        SearchRequest searchRequest = new SearchRequest("idx_test_1");
        searchRequest.source(searchSourceBuilder);

        try {
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            SearchHits hits = searchResponse.getHits();
            // the response is : {"hits":[{"score":8.8049755,"id":"1","type":"_doc","nestedIdentity":null,"version":-1,"seqNo":-2,"primaryTerm":0,"highlightFields":{},"sortValues":[],"matchedQueries":[],"explanation":null,"shard":null,"index":"idx_test_1","clusterAlias":null,"sourceAsMap":{"sex":false,"name":"curtis1","birth":null,"height":182.0},"innerHits":null,"fields":{},"sourceAsString":"{\"name\":\"curtis1\",\"birth\":null,\"sex\":false,\"height\":182.0}","rawSortValues":[],"sourceRef":{"fragment":true},"fragment":false}],"totalHits":{"value":1,"relation":"EQUAL_TO"},"maxScore":8.8049755,"sortFields":null,"collapseField":null,"collapseValues":null,"fragment":true}
            LOGGER.info("the response is : {}", new ObjectMapper().writeValueAsString(hits));

            for (SearchHit searchHit : hits.getHits()) {
                Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
                String sourceAsString = searchHit.getSourceAsString();
                // the response is : {sex=false, name=curtis1, birth=null, height=182.0}
                LOGGER.info("the response is : {}", sourceAsMap);
                // the response is : {"name":"curtis1","birth":null,"sex":false,"height":182.0}
                LOGGER.info("the response is : {}", sourceAsString);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

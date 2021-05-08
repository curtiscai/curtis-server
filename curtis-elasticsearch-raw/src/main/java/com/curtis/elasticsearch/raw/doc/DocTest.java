package com.curtis.elasticsearch.raw.doc;

import com.curtis.elasticsearch.raw.entity.User;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import org.apache.http.HttpHost;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;

/**
 * @author curtis.cai
 * @desc 文档相关操作
 * @date 2021-05-07
 * @email curtis.cai@outlook.com
 * @reference
 */
public class DocTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(DocTest.class);

    // 主机名
    private static final String HOST_NAME = "192.168.2.101";

    // 端口号
    private static final int PORT = 9200;


    /**
     * 创建文档 - 不指定id
     * <p>
     * curl --location --request POST 'http://node101:9200/idx_test/_doc' \
     * --header 'Content-Type: application/json' \
     * --data-raw '{"name":"curtis1","sex":true,"birth":"1990-01-01","phone":"17600010001","height":180.1,"desc":"我是中国人"}'
     */
    @Test
    public void testCreateDocWithoutId() {
        // 1. 创建RestHighLevelClient客户端
        // HttpHost[] hosts = new HttpHost[1];
        HttpHost httpHost = new HttpHost(HOST_NAME, PORT);
        RestClientBuilder restClientBuilder = RestClient.builder(httpHost);
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);

        // 2. 执行操作
        // 创建对象并序列化为JSON
        User user = new User("curtis1", true, "1990-01-01", 17600010001L, new BigDecimal("180.1"), "我是中国人");
        String userJson = new Gson().toJson(user);

        IndexRequest indexRequest = new IndexRequest("idx_test");
        // indexRequest.id("1");
        indexRequest.source(userJson, XContentType.JSON);
        try {
            IndexResponse indexResponse = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
            // the response is : {"shardInfo":{"total":2,"successful":1,"failures":[],"failed":0,"fragment":false},"shardId":{"index":{"name":"idx_test","uuid":"_na_","fragment":false},"indexName":"idx_test","id":-1,"fragment":true},"id":"E8C7THkBosWTVaNJx9pj","type":"_doc","version":1,"seqNo":0,"primaryTerm":1,"result":"CREATED","index":"idx_test","fragment":false}
            LOGGER.info("the response is : {}", new ObjectMapper().writeValueAsString(indexResponse));
            // the response is : CREATED
            LOGGER.info("the response is : {}", indexResponse.getResult());
            // the response is : E8C7THkBosWTVaNJx9pj
            LOGGER.info("the response is : {}", indexResponse.getId());
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 3. 关闭客户端
        try {
            restHighLevelClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建文档 - 指定id
     * <p>
     * curl --location --request PUT 'http://node101:9200/idx_test/_doc/1' \
     * --header 'Content-Type: application/json' \
     * --data-raw '{"name":"curtis1","sex":true,"birth":"1990-01-01","phone":"17600010001","height":180.1,"desc":"我是中国人"}'
     */
    @Test
    public void testCreateDocWithId() {
        // 1. 创建RestHighLevelClient客户端
        // HttpHost[] hosts = new HttpHost[1];
        HttpHost httpHost = new HttpHost(HOST_NAME, PORT);
        RestClientBuilder restClientBuilder = RestClient.builder(httpHost);
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);

        // 2. 执行操作
        // 创建对象并序列化为JSON
        User user = new User("curtis1", true, "1990-01-01", 17600010001L, new BigDecimal("180.1"), "我是中国人");
        String userJson = new Gson().toJson(user);

        IndexRequest indexRequest = new IndexRequest("idx_test");
        indexRequest.id("1");
        indexRequest.source(userJson, XContentType.JSON);
        try {
            IndexResponse indexResponse = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
            // the response is : {"shardInfo":{"total":2,"successful":1,"failures":[],"failed":0,"fragment":false},"shardId":{"index":{"name":"idx_test","uuid":"_na_","fragment":false},"indexName":"idx_test","id":-1,"fragment":true},"id":"1","type":"_doc","version":1,"seqNo":1,"primaryTerm":1,"result":"CREATED","index":"idx_test","fragment":false}
            LOGGER.info("the response is : {}", new ObjectMapper().writeValueAsString(indexResponse));
            // the response is : CREATED
            LOGGER.info("the response is : {}", indexResponse.getResult());
            // the response is : 1
            LOGGER.info("the response is : {}", indexResponse.getId());
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 3. 关闭客户端
        try {
            restHighLevelClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 查询指定文档
     * <p>
     * curl --location --request GET 'http://node101:9200/idx_test/_doc/3'
     */
    @Test
    public void testGetDocById() {
        // 1. 创建RestHighLevelClient客户端
        // HttpHost[] hosts = new HttpHost[1];
        HttpHost httpHost = new HttpHost(HOST_NAME, PORT);
        RestClientBuilder restClientBuilder = RestClient.builder(httpHost);
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);

        // 2. 执行操作
        GetRequest getRequest = new GetRequest("idx_test", "1");
        try {
            GetResponse getResponse = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
            // the response is : {"fields":{},"id":"1","type":"_doc","index":"idx_test","sourceAsString":"{\"name\":\"curtis1\",\"sex\":true,\"birth\":\"1990-01-01\",\"phone\":17600010001,\"height\":180.1,\"desc\":\"我是中国人\"}","version":1,"sourceInternal":{"fragment":true},"sourceAsBytesRef":{"fragment":true},"source":{"phone":17600010001,"sex":true,"name":"curtis1","birth":"1990-01-01","height":180.1,"desc":"我是中国人"},"primaryTerm":1,"sourceAsMap":{"phone":17600010001,"sex":true,"name":"curtis1","birth":"1990-01-01","height":180.1,"desc":"我是中国人"},"sourceEmpty":false,"exists":true,"sourceAsBytes":"eyJuYW1lIjoiY3VydGlzMSIsInNleCI6dHJ1ZSwiYmlydGgiOiIxOTkwLTAxLTAxIiwicGhvbmUiOjE3NjAwMDEwMDAxLCJoZWlnaHQiOjE4MC4xLCJkZXNjIjoi5oiR5piv5Lit5Zu95Lq6In0=","seqNo":1,"fragment":false}
            LOGGER.info("the response is : {}", new ObjectMapper().writeValueAsString(getResponse));
            // the response is : {"name":"curtis1","sex":true,"birth":"1990-01-01","phone":17600010001,"height":180.1,"desc":"我是中国人"}
            LOGGER.info("the response is : {}", getResponse.getSourceAsString());
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 3. 关闭客户端
        try {
            restHighLevelClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 修改文档 - 全量修改(与创建文档同方法)
     * <p>
     * curl --location --request PUT 'http://node101:9200/idx_test/_doc/1' \
     * --header 'Content-Type: application/json' \
     * --data-raw '{"name":"curtis1","sex":true,"birth":"1990-01-01","phone":"17600010001","height":180.1,"desc":"我是中国人"}'
     */
    @Test
    public void testUpdateAllDocById() {
        // 1. 创建RestHighLevelClient客户端
        // HttpHost[] hosts = new HttpHost[1];
        HttpHost httpHost = new HttpHost(HOST_NAME, PORT);
        RestClientBuilder restClientBuilder = RestClient.builder(httpHost);
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);

        // 2. 执行操作
        // 创建对象并序列化为JSON
        User user = new User("curtis1", null, null, 17600010001L, new BigDecimal("180.1"), "我是中国人");
        String userJson = new Gson().toJson(user);

        IndexRequest indexRequest = new IndexRequest("idx_test");
        indexRequest.id("1");
        indexRequest.source(userJson, XContentType.JSON);
        try {
            IndexResponse indexResponse = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
            // the response is : {"shardInfo":{"total":2,"successful":1,"failures":[],"failed":0,"fragment":false},"shardId":{"index":{"name":"idx_test","uuid":"_na_","fragment":false},"id":-1,"indexName":"idx_test","fragment":true},"id":"1","type":"_doc","version":2,"seqNo":2,"primaryTerm":1,"result":"UPDATED","index":"idx_test","fragment":false}
            LOGGER.info("the response is : {}", new ObjectMapper().writeValueAsString(indexResponse));
            // the response is : UPDATED
            LOGGER.info("the response is : {}", indexResponse.getResult());
            // the response is : 2
            LOGGER.info("the response is : {}", indexResponse.getVersion());
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 3. 关闭客户端
        try {
            restHighLevelClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 修改文档 - 局部修改(非空字段才会更新)
     * <p>
     * curl --location --request POST 'http://node101:9200/idx_test/_update/1' \
     * --header 'Content-Type: application/json' \
     * --data-raw '{"doc":{"name":"curtis101"}}'
     */
    @Test
    public void testUpdateNotNullDocById() {
        // 1. 创建RestHighLevelClient客户端
        // HttpHost[] hosts = new HttpHost[1];
        HttpHost httpHost = new HttpHost(HOST_NAME, PORT);
        RestClientBuilder restClientBuilder = RestClient.builder(httpHost);
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);

        // 2. 执行操作
        User user = new User("curtis101", null, null, null, null, null);
        String userJson = new Gson().toJson(user);

        UpdateRequest updateRequest = new UpdateRequest("idx_test", "1");
        updateRequest.doc(userJson, XContentType.JSON);
        try {
            UpdateResponse updateResponse = restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
            // the response is : {"shardInfo":{"total":2,"successful":1,"failures":[],"failed":0,"fragment":false},"shardId":{"index":{"name":"idx_test","uuid":"_na_","fragment":false},"id":-1,"indexName":"idx_test","fragment":true},"id":"1","type":"_doc","version":3,"seqNo":3,"primaryTerm":1,"result":"UPDATED","getResult":null,"index":"idx_test","fragment":false}
            LOGGER.info("the response is : {}", new ObjectMapper().writeValueAsString(updateResponse));
            // the response is : UPDATED
            LOGGER.info("the response is : {}", updateResponse.getResult());
            // the response is : 3
            LOGGER.info("the response is : {}", updateResponse.getVersion());
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 3. 关闭客户端
        try {
            restHighLevelClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除指定文档
     * <p>
     * curl --location --request DELETE 'http://node101:9200/idx_test/_doc/3'
     */
    @Test
    public void testDeleteDocById() {
        // 1. 创建RestHighLevelClient客户端
        // HttpHost[] hosts = new HttpHost[1];
        HttpHost httpHost = new HttpHost(HOST_NAME, PORT);
        RestClientBuilder restClientBuilder = RestClient.builder(httpHost);
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);

        // 2. 执行操作
        DeleteRequest deleteRequest = new DeleteRequest("idx_test", "1");
        try {
            DeleteResponse deleteResponse = restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
            // the response is : {"shardInfo":{"total":2,"successful":1,"failures":[],"failed":0,"fragment":false},"shardId":{"index":{"name":"idx_test","uuid":"_na_","fragment":false},"id":-1,"indexName":"idx_test","fragment":true},"id":"1","type":"_doc","version":4,"seqNo":4,"primaryTerm":1,"result":"DELETED","index":"idx_test","fragment":false}
            LOGGER.info("the response is : {}", new ObjectMapper().writeValueAsString(deleteResponse));
            // the response is : DELETED
            LOGGER.info("the response is : {}", deleteResponse.getResult());
            // the response is : 4
            LOGGER.info("the response is : {}", deleteResponse.getVersion());
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 3. 关闭客户端
        try {
            restHighLevelClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}

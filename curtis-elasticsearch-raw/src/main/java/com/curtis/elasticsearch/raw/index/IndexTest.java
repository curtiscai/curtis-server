package com.curtis.elasticsearch.raw.index;

import com.google.gson.Gson;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author curtis.cai
 * @desc 索引相关操作
 * @date 2021-05-07
 * @email curtis.cai@outlook.com
 * @reference
 */
public class IndexTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexTest.class);

    // 主机名
    private static final String HOST_NAME = "192.168.2.101";

    // 端口号
    private static final int PORT = 9200;


    /**
     * 创建索引 - 不指定mapping
     * <p>
     * curl --location --request PUT 'http://node101:9200/idx_test'
     */
    @Test
    public void testCreateIndexWithoutMapping() {
        // 1. 创建RestHighLevelClient客户端
        // HttpHost[] hosts = new HttpHost[1];
        HttpHost httpHost = new HttpHost(HOST_NAME, PORT);
        RestClientBuilder restClientBuilder = RestClient.builder(httpHost);
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);

        // 2. 执行操作
        // 获取RestHighLevelClient的用于专门处理索引的封装对象IndicesClient
        IndicesClient indicesClient = restHighLevelClient.indices();
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("idx_test");
        try {
            CreateIndexResponse createIndexResponse = indicesClient.create(createIndexRequest, RequestOptions.DEFAULT);
            // the response is : {"index":"idx_test","shardsAcknowledged":true,"acknowledged":true}
            LOGGER.info("the response is : {}", new Gson().toJson(createIndexResponse));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ElasticsearchStatusException e) {
            // 重复创建索引将抛出ElasticsearchStatusException异常，其中RestStatus是错误信息枚举类
            RestStatus restStatus = e.status();
            // the response is : BAD_REQUEST
            LOGGER.info("the response is : {}", restStatus);
            // the response is : Elasticsearch exception [type=resource_already_exists_exception, reason=index [idx_test/6oyDRGiyQ7-gEwd8t4KkYA] already exists]
            LOGGER.info("the response is : {}", e.getLocalizedMessage());
        }

        // 3. 关闭客户端
        try {
            restHighLevelClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建索引 - 指定mapping
     * <p>
     * curl --location --request PUT 'http://node101:9200/idx_test' \
     * --header 'Content-Type: application/json' \
     * --data-raw '{"settings":{"number_of_shards":1,"number_of_replicas":1},"mappings":{"properties":{"name":{"type":"keyword"},"sex":{"type":"boolean"},"birth":{"type":"date"},"phone":{"type":"long"},"height":{"type":"scaled_float","scaling_factor":100},"desc":{"type":"text"}}}}'
     */
    @Test
    public void testCreateIndexWithMapping() {
        // 1. 创建RestHighLevelClient客户端
        // HttpHost[] hosts = new HttpHost[1];
        HttpHost httpHost = new HttpHost(HOST_NAME, PORT);
        RestClientBuilder restClientBuilder = RestClient.builder(httpHost);
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);

        // 2. 执行操作
        // 获取RestHighLevelClient的用于专门处理索引的封装对象IndicesClient
        IndicesClient indicesClient = restHighLevelClient.indices();
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("idx_test_1");

        String settings = "{\"number_of_shards\":1,\"number_of_replicas\":1}";
        createIndexRequest.settings(settings, XContentType.JSON);

        String mappings = "{\"properties\":{\"name\":{\"type\":\"keyword\"},\"sex\":{\"type\":\"boolean\"},\"birth\":{\"type\":\"date\"},\"phone\":{\"type\":\"long\"},\"height\":{\"type\":\"scaled_float\",\"scaling_factor\":100},\"desc\":{\"type\":\"text\"}}}";
        createIndexRequest.mapping(mappings, XContentType.JSON);
        try {
            CreateIndexResponse createIndexResponse = indicesClient.create(createIndexRequest, RequestOptions.DEFAULT);
            // the response is : {"index":"idx_test","shardsAcknowledged":true,"acknowledged":true}
            LOGGER.info("the response is : {}", new Gson().toJson(createIndexResponse));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ElasticsearchStatusException e) {
            // 重复创建索引将抛出ElasticsearchStatusException异常，其中RestStatus是错误信息枚举类
            RestStatus restStatus = e.status();
            // the response is : BAD_REQUEST
            LOGGER.info("the response is : {}", restStatus);
            // the response is : Elasticsearch exception [type=resource_already_exists_exception, reason=index [idx_test/6oyDRGiyQ7-gEwd8t4KkYA] already exists]
            LOGGER.info("the response is : {}", e.getLocalizedMessage());
        }

        // 3. 关闭客户端
        try {
            restHighLevelClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 查询指定索引
     * <p>
     * curl --location --request GET 'http://node101:9200/idx_test'
     */
    @Test
    public void testQueryIndex() {
        // 1. 创建RestHighLevelClient客户端
        // HttpHost[] hosts = new HttpHost[1];
        HttpHost httpHost = new HttpHost(HOST_NAME, PORT);
        RestClientBuilder restClientBuilder = RestClient.builder(httpHost);
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);

        // 2. 执行操作
        // 获取RestHighLevelClient的用于专门处理索引的封装对象IndicesClient
        IndicesClient indicesClient = restHighLevelClient.indices();
        GetIndexRequest getIndexRequest = new GetIndexRequest("idx_test");
        try {
            GetIndexResponse getIndexResponse = indicesClient.get(getIndexRequest, RequestOptions.DEFAULT);
            // the response is : {"mappings":{"idx_test":{"type":"_doc","source":{"bytes":[68,70,76,0,-86,-82,5,0,0,0,-1,-1,3,0],"crc32":-1549353149},"routing":{"required":false}}},"aliases":{"idx_test":[]},"settings":{"idx_test":{"settings":{"index.creation_date":"1620476856308","index.number_of_replicas":"1","index.number_of_shards":"1","index.provided_name":"idx_test","index.routing.allocation.include._tier_preference":"data_content","index.uuid":"6oyDRGiyQ7-gEwd8t4KkYA","index.version.created":"7110299"},"firstLevelNames":{"set":{}},"keys":{"set":{}}}},"defaultSettings":{},"dataStreams":{},"indices":["idx_test"]}
            LOGGER.info("the response is : {}", new Gson().toJson(getIndexResponse));
            // the response is : {"idx_test":{"type":"_doc","source":{"bytes":[68,70,76,0,-86,-82,5,0,0,0,-1,-1,3,0],"crc32":-1549353149},"routing":{"required":false}}}
            LOGGER.info("the response is : {}", new Gson().toJson(getIndexResponse.getMappings()));
            // the response is : {idx_test={"index.creation_date":"1620476856308","index.number_of_replicas":"1","index.number_of_shards":"1",
            // "index.provided_name":"idx_test","index.routing.allocation.include._tier_preference":"data_content","index.uuid":"6oyDRGiyQ7-gEwd8t4KkYA","index.version.created":"7110299"}}
            LOGGER.info("the response is : {}", getIndexResponse.getSettings());
            // the response is : {idx_test=[]}
            LOGGER.info("the response is : {}", getIndexResponse.getAliases());
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
     * 查看索引是否存在
     * <p>
     * curl --location --head 'http://node101:9200/idx_test'
     */
    @Test
    public void testExistsIndex() {
        // 1. 创建RestHighLevelClient客户端
        // HttpHost[] hosts = new HttpHost[1];
        HttpHost httpHost = new HttpHost(HOST_NAME, PORT);
        RestClientBuilder restClientBuilder = RestClient.builder(httpHost);
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);

        // 2. 执行操作
        // 获取RestHighLevelClient的用于专门处理索引的封装对象IndicesClient
        IndicesClient indicesClient = restHighLevelClient.indices();
        GetIndexRequest getIndexRequest = new GetIndexRequest("idx_test");
        try {
            boolean exists = indicesClient.exists(getIndexRequest, RequestOptions.DEFAULT);
            // 索引存在时：  the response is : true
            // 索引不存在时：the response is : false
            LOGGER.info("the response is : {}", exists);
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
     * 删除指定索引
     * <p>
     * curl --location --request DELETE 'http://node101:9200/idx_test'
     */
    @Test
    public void testDeleteIndex() {
        // 1. 创建RestHighLevelClient客户端
        // HttpHost[] hosts = new HttpHost[1];
        HttpHost httpHost = new HttpHost(HOST_NAME, PORT);
        RestClientBuilder restClientBuilder = RestClient.builder(httpHost);
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);

        // 2. 执行操作
        // 获取RestHighLevelClient的用于专门处理索引的封装对象IndicesClient
        IndicesClient indicesClient = restHighLevelClient.indices();
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("idx_test");

        try {
            AcknowledgedResponse acknowledgedResponse = indicesClient.delete(deleteIndexRequest, RequestOptions.DEFAULT);
            // the response is : {"acknowledged":true,"fragment":false}
            LOGGER.info("the response is : {}", new Gson().toJson(acknowledgedResponse));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ElasticsearchStatusException e) {
            // 删除不存在的索引将抛出ElasticsearchStatusException异常，其中RestStatus是错误信息枚举类
            RestStatus restStatus = e.status();
            // the response is : NOT_FOUND
            LOGGER.info("the response is : {}", restStatus);
            // the response is : Elasticsearch exception [type=index_not_found_exception, reason=no such index [idx_test]]
            LOGGER.info("the response is : {}", e.getLocalizedMessage());
        }

        // 3. 关闭客户端
        try {
            restHighLevelClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

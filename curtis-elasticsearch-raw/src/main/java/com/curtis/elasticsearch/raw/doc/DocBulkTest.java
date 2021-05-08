package com.curtis.elasticsearch.raw.doc;

import com.curtis.elasticsearch.raw.entity.User;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
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
import java.util.List;

/**
 * @author curtis.cai
 * @desc 文档批量相关操作
 * @date 2021-05-08
 * @email curtis.cai@outlook.com
 * @reference
 */
public class DocBulkTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(DocBulkTest.class);

    // 主机名
    private static final String HOST_NAME = "192.168.2.101";

    // 端口号
    private static final int PORT = 9200;

    /**
     * 批量操作：批量创建/更新
     *
     * @throws JsonProcessingException
     */
    @Test
    public void testCreateUpdateBulkDoc(){
        // 1. 创建RestHighLevelClient客户端
        // HttpHost[] hosts = new HttpHost[1];
        HttpHost httpHost = new HttpHost(HOST_NAME, PORT);
        RestClientBuilder restClientBuilder = RestClient.builder(httpHost);
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);

        // 2. 执行操作
        List<User> userList = Lists.newArrayList();
        for (int i = 1; i <= 100000; i++) {
            // 保证数据是从1到10循环
            int factor = i % 10 == 0 ? 10 : i % 10;
            // 保证数据从"01"到"10"循环
            String factorStr = i % 10 == 0 ? "10" : "0" + (i % 10);
            userList.add(new User("curtis" + i, i % 2 != 0, "1990-01-" + factorStr, 17600010000L + factor, BigDecimal.valueOf(180.1 + factor), "我是中国人" + factor));
        }

        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 1; i <= 100000; i++) {
            IndexRequest indexRequest = new IndexRequest("idx_test");
            indexRequest.id(String.valueOf(i));
            indexRequest.source(new Gson().toJson(userList.get(i - 1)), XContentType.JSON);
            bulkRequest.add(indexRequest);
        }

        BulkResponse bulkResponse = null;
        try {
            bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // the response is : OK
        LOGGER.info("the response is : {}", bulkResponse.status());
        // the response is : false
        LOGGER.info("the response is : {}", bulkResponse.hasFailures());
        // the response is : 13ms
        LOGGER.info("the response is : {}", bulkResponse.getTook());


        // 3. 关闭客户端
        try {
            restHighLevelClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 批量操作：批量创建/更新
     *
     * @throws JsonProcessingException
     */
    @Test
    public void testDeleteBulkDoc() throws JsonProcessingException {
        // 1. 创建RestHighLevelClient客户端
        // HttpHost[] hosts = new HttpHost[1];
        HttpHost httpHost = new HttpHost(HOST_NAME, PORT);
        RestClientBuilder restClientBuilder = RestClient.builder(httpHost);
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);

        // 2. 执行操作
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 1; i <= 100; i++) {
            DeleteRequest deleteRequest = new DeleteRequest("idx_test",String.valueOf(i));
            bulkRequest.add(deleteRequest);
        }

        BulkResponse bulkResponse = null;
        try {
            bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // the response is : OK
        LOGGER.info("the response is : {}", bulkResponse.status());
        // the response is : false
        LOGGER.info("the response is : {}", bulkResponse.hasFailures());
        // the response is : 10ms
        LOGGER.info("the response is : {}", bulkResponse.getTook());

        // 3. 关闭客户端
        try {
            restHighLevelClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

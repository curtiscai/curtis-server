package com.curtis.elasticsearch.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;

@SpringBootTest
@RunWith(SpringRunner.class)
public class IndexTest {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexTest.class);

    /**
     * 创建索引
     */
    @Test
    public void testCreateIndex() {
        //
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("idx_test_1");

        IndicesClient indicesClient = restHighLevelClient.indices();
        try {
            CreateIndexResponse createIndexResponse = indicesClient.create(createIndexRequest, RequestOptions.DEFAULT);
            // the response is : {"acknowledged":true,"shardsAcknowledged":true,"fragment":false}
            LOGGER.info("the response is : {}", new ObjectMapper().writeValueAsString(createIndexResponse));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 查看索引是否存在
     */
    @Test
    public void testExistsIndex() {
        //
        GetIndexRequest getIndexRequest = new GetIndexRequest("idx_test_1");

        IndicesClient indicesClient = restHighLevelClient.indices();
        try {
            boolean exists = indicesClient.exists(getIndexRequest, RequestOptions.DEFAULT);
            // the response is : true
            LOGGER.info("the response is : {}", exists);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除索引
     */
    @Test
    public void testDeleteIndex() {
        //
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("idx_test_1");

        IndicesClient indicesClient = restHighLevelClient.indices();
        try {
            AcknowledgedResponse acknowledgedResponse = indicesClient.delete(deleteIndexRequest, RequestOptions.DEFAULT);
            // the response is : {"acknowledged":true,"fragment":false}
            LOGGER.info("the response is : {}", new ObjectMapper().writeValueAsString(acknowledgedResponse));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

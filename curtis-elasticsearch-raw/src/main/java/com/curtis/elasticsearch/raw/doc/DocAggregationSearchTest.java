package com.curtis.elasticsearch.raw.doc;

import com.curtis.elasticsearch.raw.entity.User;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.apache.http.HttpHost;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.metrics.Cardinality;
import org.elasticsearch.search.aggregations.metrics.ExtendedStats;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;

/**
 * @author curtis.cai
 * @desc 查询文档 - 聚合分析
 * @date 2021-05-016
 * @email curtis.cai@outlook.com
 * @reference
 */
public class DocAggregationSearchTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(DocAggregationSearchTest.class);

    // 主机名
    private static final String HOST_NAME = "192.168.2.101";

    // 端口号
    private static final int PORT = 9200;

    /**
     * 测试数据准备：插入10条数据
     */
    @Test
    public void testCreateUpdateBulkDoc() {
        // 1. 创建RestHighLevelClient客户端
        // HttpHost[] hosts = new HttpHost[1];
        HttpHost httpHost = new HttpHost(HOST_NAME, PORT);
        RestClientBuilder restClientBuilder = RestClient.builder(httpHost);
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);


        // 获取RestHighLevelClient的用于专门处理索引的封装对象IndicesClient
        IndicesClient indicesClient = restHighLevelClient.indices();

        // 2. 如果索引存在则删除
        GetIndexRequest getIndexRequest = new GetIndexRequest("idx_test");
        try {
            boolean exists = indicesClient.exists(getIndexRequest, RequestOptions.DEFAULT);
            if (exists) {
                DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("idx_test");
                AcknowledgedResponse acknowledgedResponse = indicesClient.delete(deleteIndexRequest, RequestOptions.DEFAULT);
                // the response is : {"acknowledged":true,"fragment":false}
                LOGGER.info("the response is : {}", new Gson().toJson(acknowledgedResponse));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 3. 创建索引
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("idx_test");

        String settings = "{\"number_of_shards\":1,\"number_of_replicas\":1}";
        createIndexRequest.settings(settings, XContentType.JSON);

        String mappings = "{\"properties\":{\"name\":{\"type\":\"keyword\"},\"sex\":{\"type\":\"boolean\"},\"birth\":{\"type\":\"date\"},\"phone\":{\"type\":\"long\"},\"height\":{\"type\":\"scaled_float\",\"scaling_factor\":100},\"cityName\":{\"type\":\"keyword\"},\"desc\":{\"type\":\"text\"}}}";
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

        // 4. 批量创建索引数据
        List<User> userList = Lists.newArrayList();
        for (int i = 1; i <= 10; i++) {
            // 保证数据是从1到10循环
            int factor = i % 10 == 0 ? 10 : i % 10;
            // 保证数据从"01"到"10"循环
            String factorStr = i % 10 == 0 ? "10" : "0" + (i % 10);
            String[] descArray = {"我是河北人", "我是北京人", "我是天津人", "我是上海人", "我是湖南人", "Spring是最好的框架", "MyBatis是最好的框架",
                    "我会Spring也会Redis", "Java", "C++"};
            String[] cityNameArray = {"北京", "石家庄", "天津"};
            userList.add(new User("curtis" + i, i % 2 != 0, "1990-01-" + factorStr, 17600010000L + factor,
                    BigDecimal.valueOf(180.1 + factor), cityNameArray[i % 3], descArray[i - 1]));
        }

        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 1; i <= 10; i++) {
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
     * 聚合查询 - 去重计数
     */
    @Test
    public void testSearchDocWithCardinality() {
        // 1. 创建RestHighLevelClient客户端
        // HttpHost[] hosts = new HttpHost[1];
        HttpHost httpHost = new HttpHost(HOST_NAME, PORT);
        RestClientBuilder restClientBuilder = RestClient.builder(httpHost);
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);

        // 2. 执行操作
        SearchRequest searchRequest = new SearchRequest("idx_test");
        // 去重计数
        SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource()
                .aggregation(AggregationBuilders.cardinality("agg-sex").field("sex"));
        searchRequest.source(searchSourceBuilder);
        try {
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

            // the response is : OK
            LOGGER.info("the response is : {}", searchResponse.status());
            // the response is : 1ms
            LOGGER.info("the response is : {}", searchResponse.getTook());

            SearchHits searchHits = searchResponse.getHits();

            // 获取搜索结果总数
            TotalHits totalHits = searchHits.getTotalHits();
            // the response is : 10
            LOGGER.info("the response is : {}", totalHits.value);

            // 遍历搜索结果并输出
            SearchHit[] searchHitsHitArray = searchHits.getHits();
            /*
[2021-05-16 23:55:09.979] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocAggregationSearchTest - the response is : {"name":"curtis1","sex":true,"birth":"1990-01-01","phone":17600010001,"height":181.1,"desc":"我是河北人"}
[2021-05-16 23:55:09.979] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocAggregationSearchTest - the response is : {"name":"curtis2","sex":false,"birth":"1990-01-02","phone":17600010002,"height":182.1,"desc":"我是北京人"}
[2021-05-16 23:55:09.979] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocAggregationSearchTest - the response is : {"name":"curtis3","sex":true,"birth":"1990-01-03","phone":17600010003,"height":183.1,"desc":"我是天津人"}
[2021-05-16 23:55:09.979] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocAggregationSearchTest - the response is : {"name":"curtis4","sex":false,"birth":"1990-01-04","phone":17600010004,"height":184.1,"desc":"我是上海人"}
[2021-05-16 23:55:09.979] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocAggregationSearchTest - the response is : {"name":"curtis5","sex":true,"birth":"1990-01-05","phone":17600010005,"height":185.1,"desc":"我是湖南人"}
[2021-05-16 23:55:09.979] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocAggregationSearchTest - the response is : {"name":"curtis6","sex":false,"birth":"1990-01-06","phone":17600010006,"height":186.1,"desc":"Spring是最好的框架"}
[2021-05-16 23:55:09.980] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocAggregationSearchTest - the response is : {"name":"curtis7","sex":true,"birth":"1990-01-07","phone":17600010007,"height":187.1,"desc":"MyBatis是最好的框架"}
[2021-05-16 23:55:09.980] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocAggregationSearchTest - the response is : {"name":"curtis8","sex":false,"birth":"1990-01-08","phone":17600010008,"height":188.1,"desc":"我会Spring也会Redis"}
[2021-05-16 23:55:09.980] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocAggregationSearchTest - the response is : {"name":"curtis9","sex":true,"birth":"1990-01-09","phone":17600010009,"height":189.1,"desc":"Java"}
[2021-05-16 23:55:09.980] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocAggregationSearchTest - the response is : {"name":"curtis10","sex":false,"birth":"1990-01-10","phone":17600010010,"height":190.1,"desc":"C++"}
             */
            for (SearchHit searchHit : searchHitsHitArray) {
                LOGGER.info("the response is : {}", searchHit.getSourceAsString());
            }

            Aggregations aggregations = searchResponse.getAggregations();
            Aggregation aggregation = aggregations.get("agg-sex");
            if (aggregation instanceof Cardinality) {
                Cardinality cardinality = (Cardinality) aggregation;
                // the response is : 2
                LOGGER.info("the response is : {}", cardinality.getValue());
            }
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

    @Test
    public void testSearchDocWithRangeCount() {
        // 1. 创建RestHighLevelClient客户端
        // HttpHost[] hosts = new HttpHost[1];
        HttpHost httpHost = new HttpHost(HOST_NAME, PORT);
        RestClientBuilder restClientBuilder = RestClient.builder(httpHost);
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);

        // 2. 执行操作
        SearchRequest searchRequest = new SearchRequest("idx_test");
        // 去重计数
        SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource()
                .aggregation(AggregationBuilders.range("range-height").field("height")
                        .addUnboundedTo(184.1)
                        .addRange(184.1, 188.1)
                        .addUnboundedTo(188.1))
                .size(0);
        searchRequest.source(searchSourceBuilder);
        try {
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

            // the response is : OK
            LOGGER.info("the response is : {}", searchResponse.status());
            // the response is : 0s
            LOGGER.info("the response is : {}", searchResponse.getTook());

            SearchHits searchHits = searchResponse.getHits();

            // 获取搜索结果总数
            TotalHits totalHits = searchHits.getTotalHits();
            // the response is : 10
            LOGGER.info("the response is : {}", totalHits.value);

            // 遍历搜索结果并输出
            SearchHit[] searchHitsHitArray = searchHits.getHits();

            // for (SearchHit searchHit : searchHitsHitArray) {
            //     LOGGER.info("the response is : {}", searchHit.getSourceAsString());
            // }

            Aggregation aggregation = searchResponse.getAggregations().get("range-height");
            if (aggregation instanceof Range) {
                Range heightRange = (Range) aggregation;
                List<? extends Range.Bucket> buckets = heightRange.getBuckets();
                /*
[2021-05-17 00:37:43.031] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocAggregationSearchTest - the response is : key -> *-184.1,from -> null,to -> 184.1,docCount -> 3
[2021-05-17 00:37:43.031] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocAggregationSearchTest - the response is : key -> *-188.1,from -> null,to -> 188.1,docCount -> 7
[2021-05-17 00:37:43.031] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocAggregationSearchTest - the response is : key -> 184.1-188.1,from -> 184.1,to -> 188.1,docCount -> 4
                 */
                for (Range.Bucket bucket : buckets) {
                    String keyAsString = bucket.getKeyAsString();
                    String from = bucket.getFromAsString();
                    String to = bucket.getToAsString();
                    long docCount = bucket.getDocCount();
                    LOGGER.info("the response is : key -> {},from -> {},to -> {},docCount -> {}", keyAsString, from, to, docCount);
                }
            }
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


    @Test
    public void testSearchDocWithExtendedStats() {
        // 1. 创建RestHighLevelClient客户端
        // HttpHost[] hosts = new HttpHost[1];
        HttpHost httpHost = new HttpHost(HOST_NAME, PORT);
        RestClientBuilder restClientBuilder = RestClient.builder(httpHost);
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);

        // 2. 执行操作
        SearchRequest searchRequest = new SearchRequest("idx_test");
        // 去重计数
        SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource()
                .aggregation(AggregationBuilders.extendedStats("extended_stats_height").field("height"))
                .size(0);
        searchRequest.source(searchSourceBuilder);
        try {
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

            // the response is : OK
            LOGGER.info("the response is : {}", searchResponse.status());
            // the response is : 0s
            LOGGER.info("the response is : {}", searchResponse.getTook());

            SearchHits searchHits = searchResponse.getHits();

            // 获取搜索结果总数
            TotalHits totalHits = searchHits.getTotalHits();
            // the response is : 10
            LOGGER.info("the response is : {}", totalHits.value);

            // 遍历搜索结果并输出
            SearchHit[] searchHitsHitArray = searchHits.getHits();

            // for (SearchHit searchHit : searchHitsHitArray) {
            //     LOGGER.info("the response is : {}", searchHit.getSourceAsString());
            // }

            Aggregation aggregation = searchResponse.getAggregations().get("extended_stats_height");
            if (aggregation instanceof ExtendedStats) {
                ExtendedStats heightExtendedStats = (ExtendedStats) aggregation;
                String minAsString = heightExtendedStats.getMinAsString();
                String maxAsString = heightExtendedStats.getMaxAsString();
                String avgAsString = heightExtendedStats.getAvgAsString();
                String sumAsString = heightExtendedStats.getSumAsString();
                long count = heightExtendedStats.getCount();
                // the response is : minAsString -> 181.1,maxAsString -> 190.1,avgAsString -> 185.6,sumAsString -> 1856.0,count -> 10
                LOGGER.info("the response is : minAsString -> {},maxAsString -> {},avgAsString -> {},sumAsString -> {},count -> {}",
                        minAsString, maxAsString, avgAsString, sumAsString, count);
            }
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

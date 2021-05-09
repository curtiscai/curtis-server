package com.curtis.elasticsearch.raw.doc;

import com.curtis.elasticsearch.raw.entity.User;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.apache.http.HttpHost;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;

/**
 * @author curtis.cai
 * @desc 查询文档 - 复杂查询
 * @date 2021-05-08
 * @email curtis.cai@outlook.com
 * @reference
 */
public class DocComplexSearchTest {


    private static final Logger LOGGER = LoggerFactory.getLogger(DocBulkTest.class);

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

        // 2. 执行操作
        List<User> userList = Lists.newArrayList();
        for (int i = 1; i <= 10; i++) {
            // 保证数据是从1到10循环
            int factor = i % 10 == 0 ? 10 : i % 10;
            // 保证数据从"01"到"10"循环
            String factorStr = i % 10 == 0 ? "10" : "0" + (i % 10);
            String[] descArray = {"我是河北人", "我是北京人", "我是天津人", "我是上海人", "我是湖南人", "Spring是最好的框架", "MyBatis是最好的框架",
                    "我会Spring也会Redis", "Java", "C++"};
            userList.add(new User("curtis" + i, i % 2 != 0, "1990-01-" + factorStr, 17600010000L + factor,
                    BigDecimal.valueOf(180.1 + factor), descArray[i - 1]));
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
     * 查询文档 - 检索所有文档内容
     */
    @Test
    public void testSearchDoc() {
        // 1. 创建RestHighLevelClient客户端
        // HttpHost[] hosts = new HttpHost[1];
        HttpHost httpHost = new HttpHost(HOST_NAME, PORT);
        RestClientBuilder restClientBuilder = RestClient.builder(httpHost);
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);

        // 2. 执行操作
        SearchRequest searchRequest = new SearchRequest("idx_test");
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
            for (SearchHit searchHit : searchHitsHitArray) {
                LOGGER.info("the response is : {}", searchHit.getSourceAsString());
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

    /**
     * 查询文档 - 使用请求体 - 单条件
     * <p>
     * curl --location --request POST 'http://node101:9200/idx_test/_search' \
     * --header 'Content-Type: application/json' \
     * --data-raw '{"query":{"match":{"sex":"true"}}}'
     */
    @Test
    public void testSearchWithConditionDoc() {
        // 1. 创建RestHighLevelClient客户端
        // HttpHost[] hosts = new HttpHost[1];
        HttpHost httpHost = new HttpHost(HOST_NAME, PORT);
        RestClientBuilder restClientBuilder = RestClient.builder(httpHost);
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);

        // 2. 执行操作
        SearchRequest searchRequest = new SearchRequest("idx_test");
        // 单条件等值查询
        SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource()
                .query(QueryBuilders.matchQuery("sex", true));
        searchRequest.source(searchSourceBuilder);
        try {
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

            // the response is : OK
            LOGGER.info("the response is : {}", searchResponse.status());
            // the response is : 4ms
            LOGGER.info("the response is : {}", searchResponse.getTook());

            SearchHits searchHits = searchResponse.getHits();

            // 获取搜索结果总数
            TotalHits totalHits = searchHits.getTotalHits();
            // the response is : 5
            LOGGER.info("the response is : {}", totalHits.value);

            // 遍历搜索结果并输出
            SearchHit[] searchHitsHitArray = searchHits.getHits();
            for (SearchHit searchHit : searchHitsHitArray) {
                LOGGER.info("the response is : {}", searchHit.getSourceAsString());
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

    /**
     * 查询文档 - 过滤字段
     * <p>
     * curl --location --request POST 'http://node101:9200/idx_test/_search' \
     * --header 'Content-Type: application/json' \
     * --data-raw '{"query":{"match":{"sex":"true"}},"_source":["name","phone"]}'
     */
    @Test
    public void testSearchWithConditionWithFieldsDoc() {
        // 1. 创建RestHighLevelClient客户端
        // HttpHost[] hosts = new HttpHost[1];
        HttpHost httpHost = new HttpHost(HOST_NAME, PORT);
        RestClientBuilder restClientBuilder = RestClient.builder(httpHost);
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);

        // 2. 执行操作
        SearchRequest searchRequest = new SearchRequest("idx_test");
        SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource()
                .query(QueryBuilders.matchQuery("sex", true))
                .fetchSource(new String[]{"name", "phone"}, null);
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
            // the response is : 5
            LOGGER.info("the response is : {}", totalHits.value);

            // 遍历搜索结果并输出
            SearchHit[] searchHitsHitArray = searchHits.getHits();
            for (SearchHit searchHit : searchHitsHitArray) {
                /*
[2021-05-08 20:53:35.132] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocBulkTest - the response is : {"phone":17600010001,"name":"curtis1"}
[2021-05-08 20:53:35.132] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocBulkTest - the response is : {"phone":17600010003,"name":"curtis3"}
[2021-05-08 20:53:35.132] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocBulkTest - the response is : {"phone":17600010005,"name":"curtis5"}
[2021-05-08 20:53:35.132] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocBulkTest - the response is : {"phone":17600010007,"name":"curtis7"}
[2021-05-08 20:53:35.132] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocBulkTest - the response is : {"phone":17600010009,"name":"curtis9"}
                 */
                LOGGER.info("the response is : {}", searchHit.getSourceAsString());
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

    /**
     * 查询文档 - 分页查询
     * <p>
     * curl --location --request POST 'http://node101:9200/idx_test/_search' \
     * --header 'Content-Type: application/json' \
     * --data-raw '{"query":{"match":{"sex":"true"}},"_source":["name","phone"],"from":0,"size":2}'
     */
    @Test
    public void testSearchWithPageDoc() {
        // 1. 创建RestHighLevelClient客户端
        // HttpHost[] hosts = new HttpHost[1];
        HttpHost httpHost = new HttpHost(HOST_NAME, PORT);
        RestClientBuilder restClientBuilder = RestClient.builder(httpHost);
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);

        // 2. 执行操作
        SearchRequest searchRequest = new SearchRequest("idx_test");
        SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource()
                .query(QueryBuilders.matchQuery("sex", true))
                .fetchSource(new String[]{"name", "phone"}, null)
                .from(0)
                .size(2);
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
            // the response is : 5
            LOGGER.info("the response is : {}", totalHits.value);

            // 遍历搜索结果并输出
            SearchHit[] searchHitsHitArray = searchHits.getHits();
            for (SearchHit searchHit : searchHitsHitArray) {
                /*
[2021-05-08 20:57:48.403] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocBulkTest - the response is : {"phone":17600010001,"name":"curtis1"}
[2021-05-08 20:57:48.403] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocBulkTest - the response is : {"phone":17600010003,"name":"curtis3"}
                 */
                LOGGER.info("the response is : {}", searchHit.getSourceAsString());
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

    /**
     * 查询文档 - 查询排序
     * <p>
     * curl --location --request POST 'http://node101:9200/idx_test/_search' \
     * --header 'Content-Type: application/json' \
     * --data-raw '{"query":{"match_all":{}},"from":0,"size":8,"_source":["name","phone","height","sex"],"sort":[{"sex":{"order":"asc"}},{"height":{"order":"desc"}}]}'
     */
    @Test
    public void testSearchWithSortDoc() {
        // 1. 创建RestHighLevelClient客户端
        // HttpHost[] hosts = new HttpHost[1];
        HttpHost httpHost = new HttpHost(HOST_NAME, PORT);
        RestClientBuilder restClientBuilder = RestClient.builder(httpHost);
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);

        // 2. 执行操作
        SearchRequest searchRequest = new SearchRequest("idx_test");
        SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource()
                .query(QueryBuilders.matchAllQuery())
                .fetchSource(new String[]{"name", "phone", "height", "sex"}, null)
                .from(0)
                .size(8)
                .sort("sex", SortOrder.ASC)
                .sort("height", SortOrder.DESC);
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
            for (SearchHit searchHit : searchHitsHitArray) {
                /*
[2021-05-08 21:03:47.731] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocBulkTest - the response is : {"phone":17600010010,"sex":false,"name":"curtis10","height":190.1}
[2021-05-08 21:03:47.731] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocBulkTest - the response is : {"phone":17600010008,"sex":false,"name":"curtis8","height":188.1}
[2021-05-08 21:03:47.731] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocBulkTest - the response is : {"phone":17600010006,"sex":false,"name":"curtis6","height":186.1}
[2021-05-08 21:03:47.731] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocBulkTest - the response is : {"phone":17600010004,"sex":false,"name":"curtis4","height":184.1}
[2021-05-08 21:03:47.731] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocBulkTest - the response is : {"phone":17600010002,"sex":false,"name":"curtis2","height":182.1}
[2021-05-08 21:03:47.731] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocBulkTest - the response is : {"phone":17600010009,"sex":true,"name":"curtis9","height":189.1}
[2021-05-08 21:03:47.731] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocBulkTest - the response is : {"phone":17600010007,"sex":true,"name":"curtis7","height":187.1}
[2021-05-08 21:03:47.731] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocBulkTest - the response is : {"phone":17600010005,"sex":true,"name":"curtis5","height":185.1}
                 */
                LOGGER.info("the response is : {}", searchHit.getSourceAsString());
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

    /**
     * 查询文档 - 复合查询 - AND
     * <p>
     * curl --location --request POST 'http://node101:9200/idx_test/_search' \
     * --header 'Content-Type: application/json' \
     * --data-raw '{"query":{"bool":{"must":[{"term":{"sex":"true"}},{"term":{"phone":17600010001}}]}},"from":0,"size":8,"_source":["name","phone","height","sex"],"sort":[{"sex":{"order":"asc"}},{"height":{"order":"desc"}}]}'
     */
    @Test
    public void testComplexSearchWithMustDoc() {
        // 1. 创建RestHighLevelClient客户端
        // HttpHost[] hosts = new HttpHost[1];
        HttpHost httpHost = new HttpHost(HOST_NAME, PORT);
        RestClientBuilder restClientBuilder = RestClient.builder(httpHost);
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);

        // 2. 执行操作
        SearchRequest searchRequest = new SearchRequest("idx_test");
        SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource()
                .query(QueryBuilders.boolQuery()
                        .must(QueryBuilders.termQuery("sex", true))
                        .must(QueryBuilders.termQuery("phone", 17600010001L)))
                .fetchSource(new String[]{"name", "phone", "height", "sex"}, null)
                .from(0)
                .size(8)
                .sort("sex", SortOrder.ASC)
                .sort("height", SortOrder.DESC);
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
            for (SearchHit searchHit : searchHitsHitArray) {
                /*
[2021-05-08 21:30:22.325] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocBulkTest - the response is : {"phone":17600010001,"sex":true,"name":"curtis1","height":181.1}
                 */
                LOGGER.info("the response is : {}", searchHit.getSourceAsString());
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

    /**
     * 查询文档 - 复合查询 - OR
     * <p>
     * curl --location --request POST 'http://node101:9200/idx_test/_search' \
     * --header 'Content-Type: application/json' \
     * --data-raw '{"query":{"bool":{"should":[{"term":{"sex":"true"}},{"term":{"phone":17600010002}}]}},"from":0,"size":8,"_source":["name","phone","height","sex"],"sort":[{"sex":{"order":"asc"}},{"height":{"order":"desc"}}]}'
     */
    @Test
    public void testComplexSearchWithShouldDoc() {
        // 1. 创建RestHighLevelClient客户端
        // HttpHost[] hosts = new HttpHost[1];
        HttpHost httpHost = new HttpHost(HOST_NAME, PORT);
        RestClientBuilder restClientBuilder = RestClient.builder(httpHost);
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);

        // 2. 执行操作
        SearchRequest searchRequest = new SearchRequest("idx_test");
        SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource()
                .query(QueryBuilders.boolQuery()
                        .should(QueryBuilders.termQuery("sex", true))
                        .should(QueryBuilders.termQuery("phone", 17600010002L)))
                .fetchSource(new String[]{"name", "phone", "height", "sex"}, null)
                .from(0)
                .size(8)
                .sort("sex", SortOrder.ASC)
                .sort("height", SortOrder.DESC);
        searchRequest.source(searchSourceBuilder);
        try {
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

            // the response is : OK
            LOGGER.info("the response is : {}", searchResponse.status());
            // the response is : 3ms
            LOGGER.info("the response is : {}", searchResponse.getTook());

            SearchHits searchHits = searchResponse.getHits();

            // 获取搜索结果总数
            TotalHits totalHits = searchHits.getTotalHits();
            // the response is : 6
            LOGGER.info("the response is : {}", totalHits.value);

            // 遍历搜索结果并输出
            SearchHit[] searchHitsHitArray = searchHits.getHits();
            for (SearchHit searchHit : searchHitsHitArray) {
                /*
[2021-05-08 21:35:53.599] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocBulkTest - the response is : {"phone":17600010002,"sex":false,"name":"curtis2","height":182.1}
[2021-05-08 21:35:53.599] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocBulkTest - the response is : {"phone":17600010009,"sex":true,"name":"curtis9","height":189.1}
[2021-05-08 21:35:53.599] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocBulkTest - the response is : {"phone":17600010007,"sex":true,"name":"curtis7","height":187.1}
[2021-05-08 21:35:53.599] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocBulkTest - the response is : {"phone":17600010005,"sex":true,"name":"curtis5","height":185.1}
[2021-05-08 21:35:53.599] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocBulkTest - the response is : {"phone":17600010003,"sex":true,"name":"curtis3","height":183.1}
[2021-05-08 21:35:53.599] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocBulkTest - the response is : {"phone":17600010001,"sex":true,"name":"curtis1","height":181.1}
                 */
                LOGGER.info("the response is : {}", searchHit.getSourceAsString());
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

    /**
     * 查询文档 - 范围查询
     * <p>
     * curl --location --request POST 'http://node101:9200/idx_test/_search' \
     * --header 'Content-Type: application/json' \
     * --data-raw '{"query":{"range":{"height":{"gte":183.1,"lte":187.1}}},"from":0,"size":8,"_source":["name","phone","height","sex"],"sort":[{"sex":{"order":"asc"}},{"height":{"order":"desc"}}]}'
     */
    @Test
    public void testSearchWithRangeDoc() {
        // 1. 创建RestHighLevelClient客户端
        // HttpHost[] hosts = new HttpHost[1];
        HttpHost httpHost = new HttpHost(HOST_NAME, PORT);
        RestClientBuilder restClientBuilder = RestClient.builder(httpHost);
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);

        // 2. 执行操作
        SearchRequest searchRequest = new SearchRequest("idx_test");
        SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource()
                .query(QueryBuilders.rangeQuery("height")
                        .gte(183.1)
                        .lte(187.1)
                )
                .fetchSource(new String[]{"name", "phone", "height", "sex"}, null)
                .from(0)
                .size(8)
                .sort("sex", SortOrder.ASC)
                .sort("height", SortOrder.DESC);
        searchRequest.source(searchSourceBuilder);
        try {
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

            // the response is : OK
            LOGGER.info("the response is : {}", searchResponse.status());
            // the response is : 3ms
            LOGGER.info("the response is : {}", searchResponse.getTook());

            SearchHits searchHits = searchResponse.getHits();

            // 获取搜索结果总数
            TotalHits totalHits = searchHits.getTotalHits();
            // the response is : 5
            LOGGER.info("the response is : {}", totalHits.value);

            // 遍历搜索结果并输出
            SearchHit[] searchHitsHitArray = searchHits.getHits();
            for (SearchHit searchHit : searchHitsHitArray) {
                /*
[2021-05-08 21:40:36.671] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocBulkTest - the response is : {"phone":17600010006,"sex":false,"name":"curtis6","height":186.1}
[2021-05-08 21:40:36.671] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocBulkTest - the response is : {"phone":17600010004,"sex":false,"name":"curtis4","height":184.1}
[2021-05-08 21:40:36.671] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocBulkTest - the response is : {"phone":17600010007,"sex":true,"name":"curtis7","height":187.1}
[2021-05-08 21:40:36.671] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocBulkTest - the response is : {"phone":17600010005,"sex":true,"name":"curtis5","height":185.1}
[2021-05-08 21:40:36.671] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocBulkTest - the response is : {"phone":17600010003,"sex":true,"name":"curtis3","height":183.1}
                 */
                LOGGER.info("the response is : {}", searchHit.getSourceAsString());
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

    /**
     * 查询文档 - 全文检索 - 分词后全文检索
     * <p>
     * curl --location --request POST 'http://node101:9200/idx_test/_search' \
     * --header 'Content-Type: application/json' \
     * --data-raw '{"query":{"match":{"desc":"我是河"}},"from":0,"size":8,"_source":["name","phone","height","sex","desc"],"sort":[{"sex":{"order":"asc"}},{"height":{"order":"desc"}}]}'
     */
    @Test
    public void testFullTextSearchWithMatchDoc() {
        // 1. 创建RestHighLevelClient客户端
        // HttpHost[] hosts = new HttpHost[1];
        HttpHost httpHost = new HttpHost(HOST_NAME, PORT);
        RestClientBuilder restClientBuilder = RestClient.builder(httpHost);
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);

        // 2. 执行操作
        SearchRequest searchRequest = new SearchRequest("idx_test");
        SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource()
                .query(QueryBuilders.matchQuery("desc", "我是河"))
                .fetchSource(new String[]{"name", "phone", "height", "sex", "desc"}, null)
                .from(0)
                .size(10)
                .sort("sex", SortOrder.ASC)
                .sort("height", SortOrder.DESC);
        searchRequest.source(searchSourceBuilder);
        try {
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

            // the response is : OK
            LOGGER.info("the response is : {}", searchResponse.status());
            // the response is : 3ms
            LOGGER.info("the response is : {}", searchResponse.getTook());

            SearchHits searchHits = searchResponse.getHits();

            // 获取搜索结果总数
            TotalHits totalHits = searchHits.getTotalHits();
            // the response is : 8
            LOGGER.info("the response is : {}", totalHits.value);

            // 遍历搜索结果并输出
            SearchHit[] searchHitsHitArray = searchHits.getHits();
            for (SearchHit searchHit : searchHitsHitArray) {
                /*
[2021-05-08 22:54:47.936] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocBulkTest - the response is : {"phone":17600010008,"sex":false,"name":"curtis8","height":188.1,"desc":"我会Spring也会Redis"}
[2021-05-08 22:54:47.936] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocBulkTest - the response is : {"phone":17600010006,"sex":false,"name":"curtis6","height":186.1,"desc":"Spring是最好的框架"}
[2021-05-08 22:54:47.936] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocBulkTest - the response is : {"phone":17600010004,"sex":false,"name":"curtis4","height":184.1,"desc":"我是上海人"}
[2021-05-08 22:54:47.936] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocBulkTest - the response is : {"phone":17600010002,"sex":false,"name":"curtis2","height":182.1,"desc":"我是北京人"}
[2021-05-08 22:54:47.936] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocBulkTest - the response is : {"phone":17600010007,"sex":true,"name":"curtis7","height":187.1,"desc":"MyBatis是最好的框架"}
[2021-05-08 22:54:47.936] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocBulkTest - the response is : {"phone":17600010005,"sex":true,"name":"curtis5","height":185.1,"desc":"我是湖南人"}
[2021-05-08 22:54:47.936] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocBulkTest - the response is : {"phone":17600010003,"sex":true,"name":"curtis3","height":183.1,"desc":"我是天津人"}
[2021-05-08 22:54:47.936] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocBulkTest - the response is : {"phone":17600010001,"sex":true,"name":"curtis1","height":181.1,"desc":"我是河北人"}
                 */
                LOGGER.info("the response is : {}", searchHit.getSourceAsString());
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

    /**
     * 查询文档 - 全文检索 - 不分词全文检索（必须完整包含）
     * <p>
     * curl --location --request POST 'http://node101:9200/idx_test/_search' \
     * --header 'Content-Type: application/json' \
     * --data-raw '{"query":{"match_phrase":{"desc":"我是河"}},"from":0,"size":8,"_source":["name","phone","height","sex","desc"],"sort":[{"sex":{"order":"asc"}},{"height":{"order":"desc"}}]}'
     */
    @Test
    public void testFullTextSearchWithMatchPhraseDoc() {
        // 1. 创建RestHighLevelClient客户端
        // HttpHost[] hosts = new HttpHost[1];
        HttpHost httpHost = new HttpHost(HOST_NAME, PORT);
        RestClientBuilder restClientBuilder = RestClient.builder(httpHost);
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);

        // 2. 执行操作
        SearchRequest searchRequest = new SearchRequest("idx_test");
        SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource()
                .query(QueryBuilders.matchPhraseQuery("desc", "我是河"))
                .fetchSource(new String[]{"name", "phone", "height", "sex", "desc"}, null)
                .from(0)
                .size(10)
                .sort("sex", SortOrder.ASC)
                .sort("height", SortOrder.DESC);
        searchRequest.source(searchSourceBuilder);
        try {
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

            // the response is : OK
            LOGGER.info("the response is : {}", searchResponse.status());
            // the response is : 3ms
            LOGGER.info("the response is : {}", searchResponse.getTook());

            SearchHits searchHits = searchResponse.getHits();

            // 获取搜索结果总数
            TotalHits totalHits = searchHits.getTotalHits();
            // the response is : 5
            LOGGER.info("the response is : {}", totalHits.value);

            // 遍历搜索结果并输出
            SearchHit[] searchHitsHitArray = searchHits.getHits();
            for (SearchHit searchHit : searchHitsHitArray) {
                /*
[2021-05-08 22:54:13.177] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocBulkTest - the response is : {"phone":17600010001,"sex":true,"name":"curtis1","height":181.1,"desc":"我是河北人"}
                 */
                LOGGER.info("the response is : {}", searchHit.getSourceAsString());
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

    /**
     * 查询文档 - 前缀匹配
     * <p>
     * curl --location --request POST 'http://node101:9200/idx_test/_search' \
     * --header 'Content-Type: application/json' \
     * --data-raw '{"query":{"bool":{"must":[{"match_phrase_prefix":{"desc":"我是北"}}]}},"from":0,"size":8,"_source":["name","phone","height","sex","desc"],"sort":[{"sex":{"order":"desc"}},{"phone":{"order":"desc"}}]}'
     */
    @Test
    public void testFullTextSearchWithMatchPhrasePrefixDoc() {
        // 1. 创建RestHighLevelClient客户端
        // HttpHost[] hosts = new HttpHost[1];
        HttpHost httpHost = new HttpHost(HOST_NAME, PORT);
        RestClientBuilder restClientBuilder = RestClient.builder(httpHost);
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);

        // 2. 执行操作
        SearchRequest searchRequest = new SearchRequest("idx_test");
        SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource()
                .query(QueryBuilders.boolQuery()
                        .must(QueryBuilders.matchPhraseQuery("desc", "我是河"))
                      )
                .fetchSource(new String[]{"name", "phone", "height", "sex", "desc"}, null)
                .from(0)
                .size(8)
                .sort("sex", SortOrder.ASC)
                .sort("height", SortOrder.DESC);
        searchRequest.source(searchSourceBuilder);
        try {
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

            // the response is : OK
            LOGGER.info("the response is : {}", searchResponse.status());
            // the response is : 3ms
            LOGGER.info("the response is : {}", searchResponse.getTook());

            SearchHits searchHits = searchResponse.getHits();

            // 获取搜索结果总数
            TotalHits totalHits = searchHits.getTotalHits();
            // the response is : 5
            LOGGER.info("the response is : {}", totalHits.value);

            // 遍历搜索结果并输出
            SearchHit[] searchHitsHitArray = searchHits.getHits();
            for (SearchHit searchHit : searchHitsHitArray) {
                /*
[2021-05-09 21:57:27.431] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocBulkTest - the response is : {"phone":17600010001,"sex":true,"name":"curtis1","height":181.1,"desc":"我是河北人"}
                 */
                LOGGER.info("the response is : {}", searchHit.getSourceAsString());
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

    /**
     * 查询文档 - 模糊查询
     * <p>
     */
    @Test
    public void testFullTextSearchWithFuzzyPhraseDoc() {
        // 1. 创建RestHighLevelClient客户端
        // HttpHost[] hosts = new HttpHost[1];
        HttpHost httpHost = new HttpHost(HOST_NAME, PORT);
        RestClientBuilder restClientBuilder = RestClient.builder(httpHost);
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);

        // 2. 执行操作
        SearchRequest searchRequest = new SearchRequest("idx_test");
        SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource()
                .query(QueryBuilders.fuzzyQuery("desc", "我是海").fuzziness(Fuzziness.TWO))
                .fetchSource(new String[]{"name", "phone", "height", "sex", "desc"}, null)
                .from(0)
                .size(10)
                .sort("sex", SortOrder.ASC)
                .sort("height", SortOrder.DESC);
        searchRequest.source(searchSourceBuilder);
        try {
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

            // the response is : OK
            LOGGER.info("the response is : {}", searchResponse.status());
            // the response is : 3ms
            LOGGER.info("the response is : {}", searchResponse.getTook());

            SearchHits searchHits = searchResponse.getHits();

            // 获取搜索结果总数
            TotalHits totalHits = searchHits.getTotalHits();
            // the response is : 5
            LOGGER.info("the response is : {}", totalHits.value);

            // 遍历搜索结果并输出
            SearchHit[] searchHitsHitArray = searchHits.getHits();
            for (SearchHit searchHit : searchHitsHitArray) {
                /*
[2021-05-08 22:54:13.177] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocBulkTest - the response is : {"phone":17600010001,"sex":true,"name":"curtis1","height":181.1,"desc":"我是河北人"}
                 */
                LOGGER.info("the response is : {}", searchHit.getSourceAsString());
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

    /**
     * 查询文档 - 多字段查询
     * <p>
     *
     */
    @Test
    public void testFullTextSearchWithMultiFieldDoc() {
        // 1. 创建RestHighLevelClient客户端
        // HttpHost[] hosts = new HttpHost[1];
        HttpHost httpHost = new HttpHost(HOST_NAME, PORT);
        RestClientBuilder restClientBuilder = RestClient.builder(httpHost);
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);

        // 2. 执行操作
        SearchRequest searchRequest = new SearchRequest("idx_test");
        SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource()
                .query(QueryBuilders.multiMatchQuery("河北", "desc","name"))
                .fetchSource(new String[]{"name", "phone", "height", "sex", "desc"}, null)
                .from(0)
                .size(10)
                .sort("sex", SortOrder.ASC)
                .sort("height", SortOrder.DESC);
        searchRequest.source(searchSourceBuilder);
        try {
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

            // the response is : OK
            LOGGER.info("the response is : {}", searchResponse.status());
            // the response is : 3ms
            LOGGER.info("the response is : {}", searchResponse.getTook());

            SearchHits searchHits = searchResponse.getHits();

            // 获取搜索结果总数
            TotalHits totalHits = searchHits.getTotalHits();
            // the response is : 5
            LOGGER.info("the response is : {}", totalHits.value);

            // 遍历搜索结果并输出
            SearchHit[] searchHitsHitArray = searchHits.getHits();
            for (SearchHit searchHit : searchHitsHitArray) {
                /*
[2021-05-09 21:20:20.823] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocBulkTest - the response is : {"phone":17600010002,"sex":false,"name":"curtis2","height":182.1,"desc":"我是北京人"}
[2021-05-09 21:20:20.823] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocBulkTest - the response is : {"phone":17600010001,"sex":true,"name":"curtis1","height":181.1,"desc":"我是河北人"}
                 */
                LOGGER.info("the response is : {}", searchHit.getSourceAsString());
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

    /**
     * 查询文档 - 查询多个文档
     * <p>
     * curl --location --request POST 'http://node101:9200/idx_test/_search' \
     * --header 'Content-Type: application/json' \
     * --data-raw '{"query":{"ids":{"values":[1,2]}},"_source":["name","phone","height","sex","desc"]}'
     */
    @Test
    public void testGetDocByIds() {
        // 1. 创建RestHighLevelClient客户端
        // HttpHost[] hosts = new HttpHost[1];
        HttpHost httpHost = new HttpHost(HOST_NAME, PORT);
        RestClientBuilder restClientBuilder = RestClient.builder(httpHost);
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);

        // 2. 执行操作
        SearchRequest searchRequest = new SearchRequest("idx_test");
        SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource()
                .query(QueryBuilders.idsQuery().addIds("1","2"))
                .fetchSource(new String[]{"name", "phone", "height", "sex", "desc"}, null);
        searchRequest.source(searchSourceBuilder);
        try {
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

            // the response is : OK
            LOGGER.info("the response is : {}", searchResponse.status());
            // the response is : 3ms
            LOGGER.info("the response is : {}", searchResponse.getTook());

            SearchHits searchHits = searchResponse.getHits();

            // 获取搜索结果总数
            TotalHits totalHits = searchHits.getTotalHits();
            // the response is : 5
            LOGGER.info("the response is : {}", totalHits.value);

            // 遍历搜索结果并输出
            SearchHit[] searchHitsHitArray = searchHits.getHits();
            for (SearchHit searchHit : searchHitsHitArray) {
                /*
[2021-05-09 21:32:13.195] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocBulkTest - the response is : {"phone":17600010001,"sex":true,"name":"curtis1","height":181.1,"desc":"我是河北人"}
[2021-05-09 21:32:13.195] [INFO] - [main] com.curtis.elasticsearch.raw.doc.DocBulkTest - the response is : {"phone":17600010002,"sex":false,"name":"curtis2","height":182.1,"desc":"我是北京人"}
                 */
                LOGGER.info("the response is : {}", searchHit.getSourceAsString());
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

package com.example.esapi;

import com.example.esapi.pojo.User;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
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
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class EsApiApplicationTests {

    // 因为自动配置和自己配置里面都有 RestHighLevelClient
    // 所以使用 Qualifier 锁定自己配置的 RestHighLevelClient 的名称
    @Qualifier("restHighLevelClient")
    @Autowired
    private RestHighLevelClient client;

    @Test
    void createIndex() {
        // 创建索引的请求，索引名为 index1
        CreateIndexRequest request = new CreateIndexRequest("index1");
        // 创建操作 index 的客户端
        IndicesClient indices = client.indices();
        try {
            // 客户端执行请求，获取响应
            CreateIndexResponse response = indices.create(request, RequestOptions.DEFAULT);
            // 如果成功，就会打印 true
            System.out.println(response.isAcknowledged());
            // 如果已经存在该 Index，则这个方法会抛出 ElasticsearchStatusException
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    void existIndex() {
        GetIndexRequest request = new GetIndexRequest("index1");
        IndicesClient indices = client.indices();
        try {
            boolean exists = indices.exists(request, RequestOptions.DEFAULT);
            System.out.println(exists ? "存在该索引" : "该索引不存在");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    void deleteIndex() {
        DeleteIndexRequest request = new DeleteIndexRequest("index3");
        try {
            AcknowledgedResponse response = client.indices().delete(request, RequestOptions.DEFAULT);
            //删除成功会打印 true
            System.out.println(response.isAcknowledged());
            // 如果不存在该 index 会抛出异常
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    void addDoc() throws IOException {
        // 创建 index 并添加文档
        // 如果已经创建了 index 就直接添加文档
        // 效果类似于：PUT user_index/_doc/1

        // 创建对象（记得要创建实体类）
        User john = new User("John", 15);
        // 使用 Jackson 的 ObjectMapper 转换对象为 JSON
        ObjectMapper mapper = new ObjectMapper();
        // 这里可能会有 JsonProcessingException
        String data = mapper.writeValueAsString(john);

        // Index 请求（IndexRequest 对象能自定义的内容比较多）
        IndexRequest request = new IndexRequest("user_index");
        // 将规则放入请求中
        request.id("1");
        request.timeout("1s");
        // 将 JSON 数据放入请求中
        request.source(data, XContentType.JSON);

        // 客户端发送请求，获取相应结果
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);

        // 可以打印状态（CREATED 之类的）
        System.out.println(response.status());
        // 可以打印返回的结果
        System.out.println(response.toString());
    }

    @Test
    void existDoc() throws IOException {
        // GET user_index/_doc/2
        GetRequest request = new GetRequest("user_index", "2");
        boolean exists = client.exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    @Test
    void getDoc() throws IOException {
        // GET user_index/_doc/2
        GetRequest request = new GetRequest("user_index", "2");
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        // 如果找不到该文档 ID 就会打印 null
        // 如果 index 不存在会抛出异常
        System.out.println(response.getSourceAsString());
    }

    @Test
    void updateDoc() throws IOException {
        /*
         * POST user_index/_update/2
         * {
         *   "doc": {"age":5}
         * }
         */
        UpdateRequest request = new UpdateRequest("user_index", "2");
        // 可以传入 JavaBean 转 JSON 来完整地更新数据
        // 也可以利用 Map 传入需要更改的部分
        Map<String, Integer> map = new HashMap<>();
        map.put("age", 5);

        request.doc(map);
        UpdateResponse response = client.update(request, RequestOptions.DEFAULT);
        System.out.println(response.status()); // 返回 OK 代表成功
    }

    @Test
    void delDoc() throws IOException {
        DeleteRequest request = new DeleteRequest("user_index", "2");
        DeleteResponse response = client.delete(request, RequestOptions.DEFAULT);
        System.out.println(response.status());
    }

    @Test
    void bulkAdd() throws IOException {
        // 插入大量数据

        // 模拟生成大量数据
        List<User> users = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            users.add(new User("name" + i, i));
        }

        // 这一个请求，可以包含多条数据，然后再由客户端批量处理
        BulkRequest bulkRequest = new BulkRequest();

        // 让该请求，批量接收新的请求
        for (User user : users) {
            // 创建正常的请求
            ObjectMapper mapper = new ObjectMapper();
            String json = mapper.writeValueAsString(user);
            IndexRequest indexRequest = new IndexRequest("user_index");
            // 如果没有设置 ID，就会变成随机 ID（类似于 ZLLAb3YBbt9OlO0B7dd）
//            indexRequest.id("1");
            indexRequest.source(json, XContentType.JSON);
            // 批量接收请求（相当 Collection）
            bulkRequest.add(indexRequest);
        }

        BulkResponse response = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(response.hasFailures() ? "失败" : "没有失败/成功");
    }

    @Test
    void search() throws IOException {
        // 使用工具类构建查询条件，注意 QueryBuilders 是复数
        TermQueryBuilder termQuery = QueryBuilders.termQuery("name", "john");

        // 使用 SearchSourceBuilder 放入查询条件
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(termQuery);
        // 高并发下，要设置 timeout
        sourceBuilder.timeout(new TimeValue(6, TimeUnit.SECONDS));
        // 还可以设置分页
        sourceBuilder.from(0);
        sourceBuilder.size(5);

        // 使用 SearchRequest 的 source 方法传入 SearchSourceBuilder
        SearchRequest request = new SearchRequest("user_index");
        request.source(sourceBuilder);

        // 客户端执行请求并返回响应
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 根据 SearchResponse 获取 hits
        SearchHits hits = response.getHits();
        // 这里把 hits 转为 JSON 打印出来
        System.out.println(new ObjectMapper().writeValueAsString(hits));
        System.out.println("-----------------遍历 hits，获取每个结果------------------");
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }
}

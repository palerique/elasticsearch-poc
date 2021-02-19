package br.com.palerique.elasticsearch.poc;

import com.google.gson.Gson;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

public class App {

    public static final String INDEX_PEOPLE = "people";

    public static void main(String[] args) throws IOException {

        try (RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", 9200, "http")))) {

            Gson gson = new Gson();
            for (int i = 0; i < 100; i++) {
                createPerson(client, gson);
            }

            List<Person> people = getAllDocs(client, gson);
            System.out.println(MessageFormat.format("people size = {0}\n{1}", people.size(), people));
        }
    }

    private static void createPerson(RestHighLevelClient client, Gson gson) throws IOException {
        Person p = Person.randomPerson();
        System.out.println(p);

        String jsonObject = gson.toJson(p);

        IndexRequest request = new IndexRequest(INDEX_PEOPLE);
        request.source(jsonObject, XContentType.JSON);

        IndexResponse response = client.index(request, RequestOptions.DEFAULT);

        System.out.println(MessageFormat.format("index = {0} | version = {1} | result = {2}",
                response.getIndex(),
                response.getVersion(),
                response.getResult()));
    }

    public static List<Person> getAllDocs(RestHighLevelClient client, Gson gson) throws IOException {
        int scrollSize = 1000;

        List<Person> esData = new ArrayList<>();
        SearchResponse response = null;

        int i = 0;
        while (response == null || response.getHits().getHits().length != 0) {

            final QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
            final SearchRequest searchRequest = new SearchRequest()
                    .indices(INDEX_PEOPLE)
                    .searchType(SearchType.DEFAULT)
                    .source(SearchSourceBuilder.searchSource().query(queryBuilder).from(i * scrollSize)
                            .fetchSource(true).size(scrollSize));

            response = client.search(searchRequest, RequestOptions.DEFAULT);
            for (SearchHit hit : response.getHits()) {
                esData.add(gson.fromJson(hit.getSourceAsString(), Person.class));
            }
            i++;
        }
        return esData;
    }
}

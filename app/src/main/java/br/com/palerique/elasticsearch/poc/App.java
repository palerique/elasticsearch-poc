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

            //            searchAll(client, gson);

            List<Person> people = getAllDocs(client, gson);
            System.out.println(MessageFormat.format("people size = {0}\n{1}", people.size(), people));
        }

        //        BoolQueryBuilder queryFilters = QueryBuilders.boolQuery()
        //                // tenantId must equal params tenantId
        //                .must(QueryBuilders.termQuery("context.service.tenantId", "sometenantid"))
        //                // limit to timeRange from params
        //                .must(QueryBuilders.rangeQuery("timestamp")
        //                        .gte(Instant.now().minus(365, ChronoUnit.DAYS).toEpochMilli())
        //                        .lte(Instant.now().plus(365, ChronoUnit.DAYS).toEpochMilli()));
        //
        //        queryFilters.must(QueryBuilders.termsQuery("activity.action",
        //                "statuspointsmodified"));
        //        queryFilters.must(QueryBuilders.existsQuery("activity.actionObject.extras.pointTotal"));
        //        queryFilters = onlyVisibleUsers(queryFilters);
        //
        //        MaxAggregationBuilder maxPointsSubAggregation = AggregationBuilders.max("max_points")
        //                .field("activity.actionObject.extras.pointTotal");
        //        TopHitsAggregationBuilder extraFieldsTopHitsSubAggregation = AggregationBuilders
        //                .topHits("extra_fields_top_hits")
        //                .sort(SortBuilders.fieldSort("timestamp").order(SortOrder.DESC))
        //                .fetchSource(new String[]{"activity.actor.name", "activity.actor.username"}, null)
        //                .size(1);
        //        TermsAggregationBuilder usersTermAggregation = AggregationBuilders
        //                .terms("users")
        //                .field("activity.actor.objectId")
        //                .size(1000)
        //                .order(BucketOrder.aggregation("max_points", false))
        //                .subAggregation(maxPointsSubAggregation)
        //                .subAggregation(extraFieldsTopHitsSubAggregation);
        //
        //        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
        //                .size(0);
        //
        //        SearchRequest searchRequest = new SearchRequest(new String[0])
        //                .source(searchSourceBuilder)
        //                .indicesOptions(IndicesOptions.lenientExpandOpen())
        //                .searchType(SearchType.QUERY_THEN_FETCH);
        //
        //        searchRequest.source().query(queryFilters).aggregation(usersTermAggregation);
        //
        //        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        //        SearchHit[] searchHits = response.getHits().getHits();
        //        List<String> results =
        //                Arrays.stream(searchHits)
        //                        .map(SearchHit::getSourceAsString)
        //                        .collect(Collectors.toList());
        //
        //        System.out.println(results);
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

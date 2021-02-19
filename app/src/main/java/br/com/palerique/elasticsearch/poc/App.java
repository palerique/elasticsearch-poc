package br.com.palerique.elasticsearch.poc;

import com.google.gson.Gson;
import java.io.IOException;
import java.text.MessageFormat;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

public class App {

    public static void main(String[] args) throws IOException {

        try (RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", 9200, "http")))) {

            for (int i = 0; i < 100; i++) {
                createPerson(client);
            }
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

    private static void createPerson(RestHighLevelClient client) throws IOException {
        Person p = Person.randomPerson();
        System.out.println(p);

        Gson gson = new Gson();
        String jsonObject = gson.toJson(p);

        IndexRequest request = new IndexRequest("people");
        request.source(jsonObject, XContentType.JSON);

        IndexResponse response = client.index(request, RequestOptions.DEFAULT);

        System.out.println(MessageFormat.format("index = {0} | version = {1} | result = {2}",
                response.getIndex(),
                response.getVersion(),
                response.getResult()));
    }

    private static BoolQueryBuilder onlyVisibleUsers(BoolQueryBuilder queryFilters) {
        return queryFilters
                // userId greater than 0, <0 users are not real but used for admin purposes
                .must(QueryBuilders.rangeQuery("activity.actor.objectId").gt(0))
                // be visible
                .must(QueryBuilders.termQuery("activity.actor.visible", true))
                // be regular or partner user
                .must(QueryBuilders.termsQuery("activity.actor.type", "regular", "partner"));
    }

    public String getGreeting() {
        return "Hello World!";
    }
}

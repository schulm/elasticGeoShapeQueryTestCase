package elastictest;

import java.io.IOException;
import java.net.InetAddress;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.EnvelopeBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.GeoShapeQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;

public class geoShapeNewEnvelope {

    String host = "localhost";
    String indexName = "test";
    String mappingFile = "mapping.json";
    String settingsFile = "settings.json";
    int port = 9300;
    int httpPort = 9200;

    private TransportClient client;

    @Before
    public void before() throws InterruptedException, IOException {
        this.client = new PreBuiltTransportClient(Settings.EMPTY).addTransportAddress(
                new TransportAddress(InetAddress.getByName(this.host), this.port));
        final CreateIndexRequestBuilder createIndexRequestBuilder = this.client.admin().indices().prepareCreate(this.indexName);
        final String mapping = IOUtils.toString(this.getClass().getClassLoader().getResourceAsStream(this.mappingFile), "UTF-8");

        createIndexRequestBuilder.addMapping("doc", mapping, XContentType.JSON);

        final String settings = IOUtils.toString(this.getClass().getClassLoader().getResourceAsStream(this.settingsFile), "UTF-8");
        createIndexRequestBuilder.setSettings(settings, XContentType.JSON);
        final CreateIndexResponse actionGet = createIndexRequestBuilder.execute().actionGet();

        final String doc =
                "{\"geo\": {\r\n" + "\"coordinates\": [\r\n" + "-33.918711,\r\n" + "18.847685\r\n" + "],\r\n" + "\"type\": \"Point\"\r\n" + "}}";

        this.client.prepareIndex(this.indexName, "doc", "testdoc").setSource(doc, XContentType.JSON).execute().actionGet();
        Thread.sleep(5000);
    }

    @After
    public void after() {
        this.client.admin().indices().delete(new DeleteIndexRequest(this.indexName)).actionGet();
        this.client.close();
    }

    @Test
    public void testGeoShapeQueryWithTransportClient() throws IOException, InterruptedException {
        final SearchRequestBuilder elasticQuery = this.client.prepareSearch(this.indexName).setTypes("doc").setQuery(this.getQuery());
        final SearchResponse searchResponse = elasticQuery.execute().actionGet();
        Assert.assertEquals(1, searchResponse.getHits().getTotalHits()); // will pass
    }

    @Test
    public void testGeoShapeQueryWithHttpClient() throws IOException {
        // search with http client
        final RestHighLevelClient httpClient = new RestHighLevelClient(RestClient.builder(new HttpHost(this.host, this.httpPort, "http")));
        final SearchRequest httpQuery = org.elasticsearch.client.Requests.searchRequest(this.indexName).types("doc");
        final SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(this.getQuery());
        httpQuery.source(sourceBuilder);
        final SearchResponse result2 = httpClient.search(httpQuery, RequestOptions.DEFAULT);
        Assert.assertEquals(1, result2.getHits().getTotalHits());
    }

    GeoShapeQueryBuilder getQuery() throws IOException {
        return QueryBuilders.geoShapeQuery(
                "geo", //
                new EnvelopeBuilder(new Coordinate(-21, 44), new Coordinate(-39, 9))).//
                relation(ShapeRelation.WITHIN);
    }
}

package elastictest;

import java.io.IOException;
import java.net.InetAddress;
import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.ShapeBuilders;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.GeoShapeQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Assert;
import org.junit.Test;
import com.vividsolutions.jts.geom.Coordinate;

public class geoShapeNewEnvelope {

    String host = "m-schup01";
    String indexName = "test";
    String mappingFile = "mapping.json";
    String settingsFile = "settings.json";
    int port = 9300;
    int httpPort = 9200;

    @Test
    public void geoShapeWithNewEnvelopeTest() throws IOException, InterruptedException {
        final TransportClient client = new PreBuiltTransportClient(Settings.EMPTY).addTransportAddress(
                new InetSocketTransportAddress(InetAddress.getByName(this.host), this.port));
        final CreateIndexRequestBuilder createIndexRequestBuilder = client.admin().indices().prepareCreate(this.indexName);
        final String mapping = IOUtils.toString(this.getClass().getClassLoader().getResourceAsStream(this.mappingFile), "UTF-8");

        createIndexRequestBuilder.addMapping("doc", mapping, XContentType.JSON);

        final String settings = IOUtils.toString(this.getClass().getClassLoader().getResourceAsStream(this.settingsFile), "UTF-8");
        createIndexRequestBuilder.setSettings(settings, XContentType.JSON);
        final CreateIndexResponse actionGet = createIndexRequestBuilder.execute().actionGet();

        final String doc =
                "{\"geo\": {\r\n" + "\"coordinates\": [\r\n" + "-33.918711,\r\n" + "18.847685\r\n" + "],\r\n" + "\"type\": \"Point\"\r\n" + "}}";

        client.prepareIndex(this.indexName, "doc", "testdoc").setSource(doc, XContentType.JSON).execute().actionGet();
        Thread.sleep(5000);

        final GeoShapeQueryBuilder query = QueryBuilders.geoShapeQuery(
                "geo", //
                ShapeBuilders.newEnvelope(new Coordinate(-21, 44), new Coordinate(-39, 9))).//
                relation(ShapeRelation.WITHIN);

        final SearchRequestBuilder elasticQuery = client.prepareSearch(this.indexName).setTypes("doc").//
                setQuery(query).//
                setSize(10000);
        final SearchResponse searchResponse = elasticQuery.execute().actionGet();
        Assert.assertEquals(0, searchResponse.getHits().getTotalHits()); // will pass

        // search with http client
        final RestHighLevelClient httpClient =
                new RestHighLevelClient(RestClient.builder(new HttpHost(this.host, this.httpPort, "http")).build());
        final Header header = new BasicHeader("bla", "bla");
        final SearchRequest httpQuery = org.elasticsearch.client.Requests.searchRequest(this.indexName);
        httpQuery.types("doc");
        final SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(query);
        httpQuery.source(sourceBuilder);
        final SearchResponse result2 = httpClient.search(httpQuery);
        Assert.assertEquals(1, result2.getHits().getTotalHits());
    }
}

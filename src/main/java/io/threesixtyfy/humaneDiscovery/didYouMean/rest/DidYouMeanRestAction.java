package io.threesixtyfy.humaneDiscovery.didYouMean.rest;

import io.threesixtyfy.humaneDiscovery.didYouMean.action.DidYouMeanAction;
import io.threesixtyfy.humaneDiscovery.didYouMean.action.DidYouMeanRequest;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestStatusToXContentListener;
import org.elasticsearch.search.SearchParseException;

public class DidYouMeanRestAction extends BaseRestHandler {

    @Inject
    public DidYouMeanRestAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(RestRequest.Method.GET, "/_didYouMean", this);
        controller.registerHandler(RestRequest.Method.POST, "/_didYouMean", this);

        controller.registerHandler(RestRequest.Method.GET, "/{index}/_didYouMean", this);
        controller.registerHandler(RestRequest.Method.POST, "/{index}/_didYouMean", this);

        controller.registerHandler(RestRequest.Method.GET, "/{index}/{type}/_didYouMean", this);
        controller.registerHandler(RestRequest.Method.POST, "/{index}/{type}/_didYouMean", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        DidYouMeanRequest didYouMeanRequest = new DidYouMeanRequest();
        DidYouMeanRestAction.parseSearchRequest(didYouMeanRequest, request, parseFieldMatcher, null);

        // client.search(didYouMeanRequest, new RestStatusToXContentListener<>(channel));

        client.execute(DidYouMeanAction.INSTANCE, didYouMeanRequest, new RestStatusToXContentListener<>(channel));
    }

    public static void parseSearchRequest(DidYouMeanRequest didYouMeanRequest, RestRequest request, ParseFieldMatcher parseFieldMatcher, BytesReference bodyContent) {
        didYouMeanRequest.indices(Strings.splitStringByCommaToArray(request.param("index")));

        if (bodyContent == null) {
            if (RestActions.hasBodyContent(request)) {
                bodyContent = RestActions.getRestContent(request);
            }
        }

        if (bodyContent != null) {
            parseSource(didYouMeanRequest, bodyContent);
        }

        String queryParam = request.param("q");

        if (queryParam != null) {
            didYouMeanRequest.query(queryParam);
        }

        didYouMeanRequest.types(Strings.splitStringByCommaToArray(request.param("type")));
        didYouMeanRequest.indicesOptions(IndicesOptions.fromRequest(request, didYouMeanRequest.indicesOptions()));
    }

    private static void parseSource(DidYouMeanRequest didYouMeanRequest, BytesReference source) throws SearchParseException {
        // nothing to parse...
        if (source == null || source.length() == 0) {
            return;
        }
        XContentParser parser = null;
        try {
            parser = XContentFactory.xContent(source).createParser(source);
            XContentParser.Token token;
            token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new ElasticsearchParseException("failed to parse. source must be an object, but found [{}] instead", token.name());
            }

            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    String fieldName = parser.currentName();

                    if (!"query".equals(fieldName)) {
                        throw new ElasticsearchParseException("failed to parse. expected 'query' but got [{}]", token);
                    }

                    token = parser.nextToken();

                    if (token.isValue()) {
                        didYouMeanRequest.query(parser.text());
                    } else {
                        throw new ElasticsearchParseException("failed to parse. expected value but got [{}]", token);
                    }
                } else {
                    if (token == null) {
                        throw new ElasticsearchParseException("failed to parse search source. end of query source reached but query is not complete.");
                    } else {
                        throw new ElasticsearchParseException("failed to parse search source. expected field name but got [{}]", token);
                    }
                }
            }
        } catch (Throwable e) {
            throw new ElasticsearchParseException("failed to parse.", e);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
    }
}

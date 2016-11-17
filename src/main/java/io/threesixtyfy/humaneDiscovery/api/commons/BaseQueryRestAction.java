package io.threesixtyfy.humaneDiscovery.api.commons;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.search.SearchParseException;

import java.io.IOException;

public abstract class BaseQueryRestAction<T extends BaseQueryRequest> extends BaseRestHandler {

    @Inject
    public BaseQueryRestAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.GET, "/" + restActionName(), this);
        controller.registerHandler(RestRequest.Method.POST, "/" + restActionName(), this);

        controller.registerHandler(RestRequest.Method.GET, "/{index}/" + restActionName(), this);
        controller.registerHandler(RestRequest.Method.POST, "/{index}/" + restActionName(), this);

        controller.registerHandler(RestRequest.Method.GET, "/{index}/{type}/" + restActionName(), this);
        controller.registerHandler(RestRequest.Method.POST, "/{index}/{type}/" + restActionName(), this);
    }

    protected abstract String restActionName();

    protected abstract T newRequest();

    protected abstract RestChannelConsumer execute(T request, final Client client);

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        T baseRequest = newRequest();

        parseRequest(baseRequest, request, parseFieldMatcher, null);

        return execute(baseRequest, client);
    }

    private void parseRequest(T baseRequest, RestRequest request, ParseFieldMatcher parseFieldMatcher, BytesReference bodyContent) {
        baseRequest.indices(Strings.splitStringByCommaToArray(request.param("index")));

        if (bodyContent == null) {
            if (RestActions.hasBodyContent(request)) {
                bodyContent = RestActions.getRestContent(request);
            }
        }

        if (bodyContent != null) {
            parseSource(baseRequest, parseFieldMatcher, bodyContent);
        }

        String queryParam = request.param("q");

        if (queryParam != null) {
            baseRequest.querySource().query(queryParam);
        }

        baseRequest.types(Strings.splitStringByCommaToArray(request.param("type")));
        baseRequest.indicesOptions(IndicesOptions.fromRequest(request, baseRequest.indicesOptions()));
    }

    private void parseSource(T baseRequest, ParseFieldMatcher parseFieldMatcher, BytesReference source) throws SearchParseException {
        // nothing to parse...
        if (source == null || source.length() == 0) {
            return;
        }
        XContentParser parser = null;
        try {
            parser = XContentFactory.xContent(source).createParser(source);

            // parse query source
            baseRequest.querySource().fromXContent(parser, parseFieldMatcher);

        } catch (Throwable e) {
            throw new ElasticsearchParseException("failed to parse.", e);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
    }
}

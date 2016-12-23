package io.threesixtyfy.humaneDiscovery.api.commons;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.search.SearchParseException;

import java.io.IOException;

public abstract class BaseRestAction<T extends BaseRequest> extends BaseRestHandler {

    @Inject
    public BaseRestAction(Settings settings, RestController controller) {
        super(settings);

        controller.registerHandler(RestRequest.Method.GET, "/{index}/" + restActionName(), this);
        controller.registerHandler(RestRequest.Method.POST, "/{index}/" + restActionName(), this);
    }

    protected abstract String restActionName();

    protected abstract T newRequest();

    protected abstract RestChannelConsumer execute(T request, final Client client);

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        T baseRequest = newRequest();

        parseRequest(baseRequest, request, parseFieldMatcher);

        return execute(baseRequest, client);
    }

    protected final void parseRequest(T baseRequest, RestRequest request, ParseFieldMatcher parseFieldMatcher) {
        baseRequest.instance(request.param("index"));

        BytesReference bodyContent = null;
        if (RestActions.hasBodyContent(request)) {
            bodyContent = RestActions.getRestContent(request);
        }

        if (bodyContent != null) {
            parseSource(baseRequest, parseFieldMatcher, bodyContent);
        }

        parseParams(baseRequest, request, parseFieldMatcher, bodyContent);
    }

    protected abstract void parseSource(T baseRequest, ParseFieldMatcher parseFieldMatcher, BytesReference source) throws SearchParseException;

    protected abstract void parseParams(T baseRequest, RestRequest request, ParseFieldMatcher parseFieldMatcher, BytesReference source) throws SearchParseException;

}

package io.threesixtyfy.humaneDiscovery.api.search;

import io.threesixtyfy.humaneDiscovery.api.commons.QueryRestAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.search.SearchParseException;

public class SearchRestAction extends QueryRestAction<SearchQuerySource, SearchQueryRequest> {

    private static final String NAME = "search";

    @Inject
    public SearchRestAction(Settings settings, RestController controller) {
        super(settings, controller);
    }

    @Override
    protected String restActionName() {
        return NAME;
    }

    @Override
    protected SearchQueryRequest newRequest() {
        return new SearchQueryRequest();
    }

    @Override
    protected RestChannelConsumer execute(SearchQueryRequest request, Client client) {
        return channel -> client.execute(SearchAction.INSTANCE, request, new RestStatusToXContentListener<>(channel));
    }

    @Override
    protected void parseParams(SearchQueryRequest baseRequest, RestRequest request, ParseFieldMatcher parseFieldMatcher, BytesReference source) throws SearchParseException {
        super.parseParams(baseRequest, request, parseFieldMatcher, source);

        for (String key : request.params().keySet()) {
            if (parseFieldMatcher.match(key, SearchConstants.COUNT_FIELD)) {
                baseRequest.querySource().count(request.paramAsInt(key, SearchConstants.DEFAULT_COUNT));
            } else if (parseFieldMatcher.match(key, SearchConstants.PAGE_FIELD)) {
                baseRequest.querySource().page(request.paramAsInt(key, SearchConstants.DEFAULT_PAGE));
            } else if (parseFieldMatcher.match(key, SearchConstants.TYPE_FIELD)) {
                baseRequest.querySource().type(request.param(key));
            } else if (parseFieldMatcher.match(key, SearchConstants.SECTION_FIELD)) {
                baseRequest.querySource().section(request.param(key));
            } else if (parseFieldMatcher.match(key, SearchConstants.FORMAT_FIELD)) {
                baseRequest.querySource().format(request.param(key));
            }
        }
    }
}

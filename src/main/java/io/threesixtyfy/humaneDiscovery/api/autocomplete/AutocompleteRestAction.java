package io.threesixtyfy.humaneDiscovery.api.autocomplete;

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

public class AutocompleteRestAction extends QueryRestAction<AutocompleteQuerySource, AutocompleteQueryRequest> {

    private static final String NAME = "autocomplete";

    @Inject
    public AutocompleteRestAction(Settings settings, RestController controller) {
        super(settings, controller);
    }

    @Override
    protected String restActionName() {
        return NAME;
    }

    @Override
    protected AutocompleteQueryRequest newRequest() {
        return new AutocompleteQueryRequest();
    }

    @Override
    protected RestChannelConsumer execute(AutocompleteQueryRequest request, Client client) {
        return channel -> client.execute(AutocompleteAction.INSTANCE, request, new RestStatusToXContentListener<>(channel));
    }

    @Override
    protected void parseParams(AutocompleteQueryRequest baseRequest, RestRequest request, ParseFieldMatcher parseFieldMatcher, BytesReference source) throws SearchParseException {
        super.parseParams(baseRequest, request, parseFieldMatcher, source);

        for (String key : request.params().keySet()) {
            if (parseFieldMatcher.match(key, AutocompleteConstants.COUNT_FIELD)) {
                baseRequest.querySource().count(request.paramAsInt(key, AutocompleteConstants.DEFAULT_COUNT));
            } else if (parseFieldMatcher.match(key, AutocompleteConstants.PAGE_FIELD)) {
                baseRequest.querySource().page(request.paramAsInt(key, AutocompleteConstants.DEFAULT_PAGE));
            } else if (parseFieldMatcher.match(key, AutocompleteConstants.TYPE_FIELD)) {
                baseRequest.querySource().type(request.param(key));
            } else if (parseFieldMatcher.match(key, AutocompleteConstants.SECTION_FIELD)) {
                baseRequest.querySource().section(request.param(key));
            } else if (parseFieldMatcher.match(key, AutocompleteConstants.FORMAT_FIELD)) {
                baseRequest.querySource().format(request.param(key));
            }
        }
    }
}

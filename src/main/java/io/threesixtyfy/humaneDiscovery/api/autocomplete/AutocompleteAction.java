package io.threesixtyfy.humaneDiscovery.api.autocomplete;

import io.threesixtyfy.humaneDiscovery.api.commons.QueryResponse;
import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

public class AutocompleteAction extends Action<AutocompleteQueryRequest, QueryResponse, AutocompleteRequestBuilder> {

    public static final AutocompleteAction INSTANCE = new AutocompleteAction();

    public static final String NAME = "indices:data/read/360fy/autocomplete";

    private AutocompleteAction() {
        super(NAME);
    }

    @Override
    public AutocompleteRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new AutocompleteRequestBuilder(client, this);
    }

    @Override
    public QueryResponse newResponse() {
        return new AutocompleteResponse();
    }
}

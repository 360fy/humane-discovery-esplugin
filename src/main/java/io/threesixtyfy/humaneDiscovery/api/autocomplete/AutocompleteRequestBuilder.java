package io.threesixtyfy.humaneDiscovery.api.autocomplete;

import io.threesixtyfy.humaneDiscovery.api.commons.BaseRequestBuilder;
import io.threesixtyfy.humaneDiscovery.api.commons.QueryResponse;
import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

public class AutocompleteRequestBuilder extends BaseRequestBuilder<AutocompleteQueryRequest, QueryResponse, AutocompleteRequestBuilder> {

    public AutocompleteRequestBuilder(ElasticsearchClient client, Action<AutocompleteQueryRequest, QueryResponse, AutocompleteRequestBuilder> action) {
        super(client, action, new AutocompleteQueryRequest());
    }

}

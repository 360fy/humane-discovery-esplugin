package io.threesixtyfy.humaneDiscovery.api.search;

import io.threesixtyfy.humaneDiscovery.api.commons.BaseRequestBuilder;
import io.threesixtyfy.humaneDiscovery.api.commons.QueryResponse;
import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

public class SearchRequestBuilder extends BaseRequestBuilder<SearchQueryRequest, QueryResponse, SearchRequestBuilder> {

    public SearchRequestBuilder(ElasticsearchClient client, Action<SearchQueryRequest, QueryResponse, SearchRequestBuilder> action) {
        super(client, action, new SearchQueryRequest());
    }

}

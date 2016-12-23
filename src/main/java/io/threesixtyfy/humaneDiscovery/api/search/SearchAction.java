package io.threesixtyfy.humaneDiscovery.api.search;

import io.threesixtyfy.humaneDiscovery.api.commons.QueryResponse;
import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

public class SearchAction extends Action<SearchQueryRequest, QueryResponse, SearchRequestBuilder> {

    public static final SearchAction INSTANCE = new SearchAction();

    public static final String NAME = "indices:data/read/360fy/search";

    private SearchAction() {
        super(NAME);
    }

    @Override
    public SearchRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new SearchRequestBuilder(client, this);
    }

    @Override
    public QueryResponse newResponse() {
        return new SingleSectionSearchResponse("");
    }
}

package io.threesixtyfy.humaneDiscovery.api.search;

import io.threesixtyfy.humaneDiscovery.api.commons.QueryRequest;

public class SearchQueryRequest extends QueryRequest<SearchQuerySource, SearchQueryRequest> {

    public SearchQueryRequest() {
        super(new SearchQuerySource());
    }

}

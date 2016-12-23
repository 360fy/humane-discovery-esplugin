package io.threesixtyfy.humaneDiscovery.api.autocomplete;

import io.threesixtyfy.humaneDiscovery.api.commons.QueryResponse;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.search.SearchHit;

public class AutocompleteResponse extends QueryResponse<AutocompleteResult> {

    private static final AutocompleteResult[] EMPTY = new AutocompleteResult[0];

    public AutocompleteResponse(String searchText) {
        super(searchText);
    }

    public AutocompleteResponse(String searchText, AutocompleteResult[] results, long totalResults) {
        super(searchText, results, totalResults);
    }

    public AutocompleteResponse(String searchText, AutocompleteResult[] results, long totalResults, int totalShards, int successfulShards, ShardOperationFailedException[] shardFailures) {
        super(searchText, results, totalResults, totalShards, successfulShards, shardFailures);
    }

    @Override
    protected AutocompleteResult newResult() {
        return new AutocompleteResult();
    }

    @Override
    protected AutocompleteResult[] newResults(int size) {
        return new AutocompleteResult[size];
    }

    @Override
    protected AutocompleteResult[] emptyResults() {
        return EMPTY;
    }

}

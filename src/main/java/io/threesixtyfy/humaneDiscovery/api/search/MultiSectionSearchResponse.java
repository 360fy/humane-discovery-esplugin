package io.threesixtyfy.humaneDiscovery.api.search;

import io.threesixtyfy.humaneDiscovery.api.commons.QueryResponse;
import org.elasticsearch.action.ShardOperationFailedException;

public class MultiSectionSearchResponse extends QueryResponse<SectionResult> {

    public static final SectionResult[] EMPTY = new SectionResult[0];

    public MultiSectionSearchResponse(String searchText) {
        super(searchText);
    }

    public MultiSectionSearchResponse(String searchText, SectionResult[] results) {
        super(searchText, results, CalculateTotalResults(results));
    }

    public MultiSectionSearchResponse(String searchText, SectionResult[] results, int totalShards, int successfulShards, ShardOperationFailedException[] shardFailures) {
        super(searchText, results, CalculateTotalResults(results), totalShards, successfulShards, shardFailures);
    }

    @Override
    protected SectionResult newResult() {
        return new SectionResult();
    }

    @Override
    protected SectionResult[] newResults(int size) {
        return new SectionResult[size];
    }

    @Override
    protected SectionResult[] emptyResults() {
        return EMPTY;
    }

    private static long CalculateTotalResults(SectionResult[] results) {
        if (results == null) {
            return 0;
        }

        long total = 0;
        for (SectionResult sectionResult : results) {
            total += sectionResult.getTotalResults();
        }

        return total;
    }

}

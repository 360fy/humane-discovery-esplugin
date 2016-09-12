package io.threesixtyfy.humaneDiscovery.didYouMean.action;

import io.threesixtyfy.humaneDiscovery.commons.action.BaseQueryResponse;

public class DidYouMeanResponse extends BaseQueryResponse<DidYouMeanResult> {

    public static final DidYouMeanResult[] EMPTY = new DidYouMeanResult[0];

    public DidYouMeanResponse() {
        super();
    }

    public DidYouMeanResponse(long tookInMillis) {
        super(tookInMillis);
    }

    public DidYouMeanResponse(DidYouMeanResult[] results, long tookInMillis) {
        super(results, tookInMillis);
    }

    @Override
    protected DidYouMeanResult newResult() {
        return new DidYouMeanResult();
    }

    @Override
    protected DidYouMeanResult[] newResults(int size) {
        return new DidYouMeanResult[size];
    }

    @Override
    protected DidYouMeanResult[] emptyResults() {
        return EMPTY;
    }
}

package io.threesixtyfy.humaneDiscovery.intent.action;

import io.threesixtyfy.humaneDiscovery.commons.action.BaseQueryResponse;

public class IntentResponse extends BaseQueryResponse<IntentResult> {

    public static final IntentResult[] EMPTY = new IntentResult[0];

    public IntentResponse() {
        super();
    }

    public IntentResponse(long tookInMillis) {
        super(tookInMillis);
    }

    public IntentResponse(IntentResult[] results, long tookInMillis) {
        super(results, tookInMillis);
    }

    @Override
    protected IntentResult newResult() {
        return new IntentResult();
    }

    @Override
    protected IntentResult[] newResults(int size) {
        return new IntentResult[size];
    }

    @Override
    protected IntentResult[] emptyResults() {
        return EMPTY;
    }
}

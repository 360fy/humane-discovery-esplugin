package io.threesixtyfy.humaneDiscovery.api.intent;

import io.threesixtyfy.humaneDiscovery.api.commons.QueryRequest;

public class IntentQueryRequest extends QueryRequest<IntentQuerySource, IntentQueryRequest> {

    public IntentQueryRequest() {
        super(new IntentQuerySource());
    }

}

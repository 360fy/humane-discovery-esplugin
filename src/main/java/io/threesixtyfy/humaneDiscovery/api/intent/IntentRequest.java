package io.threesixtyfy.humaneDiscovery.api.intent;

import io.threesixtyfy.humaneDiscovery.api.commons.BaseQueryRequest;
import org.elasticsearch.action.ActionRequest;

public class IntentRequest extends BaseQueryRequest<IntentQuerySource, IntentRequest> {

    public IntentRequest() {

    }

    public IntentRequest(IntentRequest intentRequest, ActionRequest originalRequest) {
        super(intentRequest, originalRequest);
    }

    @Override
    protected IntentQuerySource newQuerySource() {
        return new IntentQuerySource();
    }

    @Override
    public String toString() {
        return "IntentRequest{} " + super.toString();
    }
}

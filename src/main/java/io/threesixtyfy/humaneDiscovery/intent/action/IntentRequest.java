package io.threesixtyfy.humaneDiscovery.intent.action;

import io.threesixtyfy.humaneDiscovery.commons.action.BaseQueryRequest;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

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

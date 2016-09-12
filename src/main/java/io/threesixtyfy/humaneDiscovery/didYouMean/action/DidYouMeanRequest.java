package io.threesixtyfy.humaneDiscovery.didYouMean.action;

import io.threesixtyfy.humaneDiscovery.commons.action.BaseQueryRequest;
import org.elasticsearch.action.ActionRequest;

public class DidYouMeanRequest extends BaseQueryRequest<DidYouMeanQuerySource, DidYouMeanRequest> {

    public DidYouMeanRequest() {
    }

    public DidYouMeanRequest(DidYouMeanRequest didYouMeanRequest, ActionRequest originalRequest) {
        super(didYouMeanRequest, originalRequest);
    }

    @Override
    protected DidYouMeanQuerySource newQuerySource() {
        return new DidYouMeanQuerySource();
    }

}

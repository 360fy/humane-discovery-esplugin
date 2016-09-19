package io.threesixtyfy.humaneDiscovery.api.didYouMean;

import io.threesixtyfy.humaneDiscovery.api.commons.BaseQueryRequest;
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

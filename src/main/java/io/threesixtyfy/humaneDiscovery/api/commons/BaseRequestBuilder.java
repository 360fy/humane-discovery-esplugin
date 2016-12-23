package io.threesixtyfy.humaneDiscovery.api.commons;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

public abstract class BaseRequestBuilder<A extends BaseRequest, B extends BaseResponse, C extends BaseRequestBuilder<A, B, C>> extends ActionRequestBuilder<A, B, C> {

    public BaseRequestBuilder(ElasticsearchClient client, Action<A, B, C> action, A request) {
        super(client, action, request);
    }

    public BaseRequestBuilder setInstance(String instance) {
        request.instance(instance);
        return this;
    }

}

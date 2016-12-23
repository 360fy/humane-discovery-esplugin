package io.threesixtyfy.humaneDiscovery.api.intent;

import io.threesixtyfy.humaneDiscovery.api.commons.BaseRequestBuilder;
import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

public class IntentRequestBuilder extends BaseRequestBuilder<IntentQueryRequest, IntentResponse, IntentRequestBuilder> {

    public IntentRequestBuilder(ElasticsearchClient client, Action<IntentQueryRequest, IntentResponse, IntentRequestBuilder> action) {
        super(client, action, new IntentQueryRequest());
    }

}

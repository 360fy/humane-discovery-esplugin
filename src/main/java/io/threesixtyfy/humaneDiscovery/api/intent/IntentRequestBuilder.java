package io.threesixtyfy.humaneDiscovery.api.intent;

import io.threesixtyfy.humaneDiscovery.api.commons.BaseQueryRequestBuilder;
import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

public class IntentRequestBuilder extends BaseQueryRequestBuilder<IntentRequest, IntentResponse, IntentRequestBuilder> {

    public IntentRequestBuilder(ElasticsearchClient client, Action<IntentRequest, IntentResponse, IntentRequestBuilder> action) {
        super(client, action, new IntentRequest());
    }

}

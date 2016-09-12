package io.threesixtyfy.humaneDiscovery.intent.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

public class IntentAction extends Action<IntentRequest, IntentResponse, IntentRequestBuilder> {

    public static final IntentAction INSTANCE = new IntentAction();

    public static final String NAME = "indices:data/read/intent";

    protected IntentAction() {
        super(NAME);
    }

    @Override
    public IntentRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new IntentRequestBuilder(client, this);
    }

    @Override
    public IntentResponse newResponse() {
        return new IntentResponse();
    }
}

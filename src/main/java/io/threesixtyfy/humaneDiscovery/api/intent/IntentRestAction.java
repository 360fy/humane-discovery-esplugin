package io.threesixtyfy.humaneDiscovery.api.intent;

import io.threesixtyfy.humaneDiscovery.api.commons.QueryRestAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.action.RestStatusToXContentListener;

public class IntentRestAction extends QueryRestAction<IntentQuerySource, IntentQueryRequest> {

    private static final String NAME = "intent";

    @Inject
    public IntentRestAction(Settings settings, RestController controller) {
        super(settings, controller);
    }

    @Override
    protected String restActionName() {
        return NAME;
    }

    @Override
    protected IntentQueryRequest newRequest() {
        return new IntentQueryRequest();
    }

    @Override
    protected RestChannelConsumer execute(IntentQueryRequest request, Client client) {
        return channel -> client.execute(IntentAction.INSTANCE, request, new RestStatusToXContentListener<>(channel));
    }

}

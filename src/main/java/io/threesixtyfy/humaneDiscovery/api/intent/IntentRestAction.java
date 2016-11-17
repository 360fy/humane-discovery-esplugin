package io.threesixtyfy.humaneDiscovery.api.intent;

import io.threesixtyfy.humaneDiscovery.api.commons.BaseQueryRestAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.action.RestStatusToXContentListener;

public class IntentRestAction extends BaseQueryRestAction<IntentRequest> {

    private static final String NAME = "_intent";

    @Inject
    public IntentRestAction(Settings settings, RestController controller) {
        super(settings, controller);
    }

    @Override
    protected String restActionName() {
        return NAME;
    }

    @Override
    protected IntentRequest newRequest() {
        return new IntentRequest();
    }

    @Override
    protected RestChannelConsumer execute(IntentRequest request, Client client) {
        return channel -> client.execute(IntentAction.INSTANCE, request, new RestStatusToXContentListener<>(channel));
    }

}

package io.threesixtyfy.humaneDiscovery.api.intent;

import io.threesixtyfy.humaneDiscovery.api.commons.BaseQueryRestAction;
import io.threesixtyfy.humaneDiscovery.api.intent.IntentAction;
import io.threesixtyfy.humaneDiscovery.api.intent.IntentRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.action.support.RestStatusToXContentListener;

public class IntentRestAction extends BaseQueryRestAction<IntentRequest> {

    @Inject
    public IntentRestAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
    }

    @Override
    protected String restActionName() {
        return "_intent";
    }

    @Override
    protected IntentRequest newRequest() {
        return new IntentRequest();
    }

    @Override
    protected void execute(IntentRequest request, RestChannel channel, Client client) {
        client.execute(IntentAction.INSTANCE, request, new RestStatusToXContentListener<>(channel));
    }

}

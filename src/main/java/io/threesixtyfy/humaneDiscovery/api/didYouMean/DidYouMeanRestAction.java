package io.threesixtyfy.humaneDiscovery.api.didYouMean;

import io.threesixtyfy.humaneDiscovery.api.commons.BaseQueryRestAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.action.RestStatusToXContentListener;

public class DidYouMeanRestAction extends BaseQueryRestAction<DidYouMeanRequest> {

    private static final String NAME = "_didYouMean";

    @Inject
    public DidYouMeanRestAction(Settings settings, RestController controller) {
        super(settings, controller);
    }

    @Override
    protected String restActionName() {
        return NAME;
    }

    @Override
    protected DidYouMeanRequest newRequest() {
        return new DidYouMeanRequest();
    }

    @Override
    protected RestChannelConsumer execute(DidYouMeanRequest request, Client client) {
        return channel -> client.execute(DidYouMeanAction.INSTANCE, request, new RestStatusToXContentListener<>(channel));
    }

}

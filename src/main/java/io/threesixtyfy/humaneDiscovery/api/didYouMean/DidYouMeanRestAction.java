package io.threesixtyfy.humaneDiscovery.api.didYouMean;

import io.threesixtyfy.humaneDiscovery.api.commons.BaseQueryRestAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.action.support.RestStatusToXContentListener;

public class DidYouMeanRestAction extends BaseQueryRestAction<DidYouMeanRequest> {

    @Inject
    public DidYouMeanRestAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
    }

    @Override
    protected String restActionName() {
        return "_didYouMean";
    }

    @Override
    protected DidYouMeanRequest newRequest() {
        return new DidYouMeanRequest();
    }

    @Override
    protected void execute(DidYouMeanRequest request, RestChannel channel, Client client) {
        client.execute(DidYouMeanAction.INSTANCE, request, new RestStatusToXContentListener<>(channel));
    }

}

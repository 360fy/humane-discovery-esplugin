package io.threesixtyfy.humaneDiscovery.api.didYouMean;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

public class DidYouMeanAction extends Action<DidYouMeanRequest, DidYouMeanResponse, DidYouMeanRequestBuilder> {

    public static final DidYouMeanAction INSTANCE = new DidYouMeanAction();

    public static final String NAME = "indices:data/read/didYouMean";

    private DidYouMeanAction() {
        super(NAME);
    }

    @Override
    public DidYouMeanRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new DidYouMeanRequestBuilder(client, this);
    }

    @Override
    public DidYouMeanResponse newResponse() {
        return new DidYouMeanResponse();
    }
}

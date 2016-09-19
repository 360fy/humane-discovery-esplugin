package io.threesixtyfy.humaneDiscovery.api.didYouMean;

import io.threesixtyfy.humaneDiscovery.api.commons.BaseQueryRequestBuilder;
import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

public class DidYouMeanRequestBuilder extends BaseQueryRequestBuilder<DidYouMeanRequest, DidYouMeanResponse, DidYouMeanRequestBuilder> {

    public DidYouMeanRequestBuilder(ElasticsearchClient client, Action<DidYouMeanRequest, DidYouMeanResponse, DidYouMeanRequestBuilder> action) {
        super(client, action, new DidYouMeanRequest());
    }

}

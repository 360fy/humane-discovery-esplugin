package io.threesixtyfy.humaneDiscovery.commons.action;

import io.threesixtyfy.humaneDiscovery.didYouMean.action.DidYouMeanRequestBuilder;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.ElasticsearchClient;

public abstract class BaseQueryRequestBuilder<A extends BaseQueryRequest, B extends BaseQueryResponse, C extends BaseQueryRequestBuilder<A, B, C>> extends ActionRequestBuilder<A, B, C> {

    public BaseQueryRequestBuilder(ElasticsearchClient client, Action<A, B, C> action, A request) {
        super(client, action, request);
    }

    /**
     * Sets the indices the search will be executed on.
     */
    public BaseQueryRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    /**
     * Specifies what type of requested indices to ignore and wildcard indices expressions.
     * <p>
     * For example indices that don't exist.
     */
    public BaseQueryRequestBuilder setIndicesOptions(IndicesOptions indicesOptions) {
        request().indicesOptions(indicesOptions);
        return this;
    }

    /**
     * The document types to execute the search against. Defaults to be executed against
     * all types.
     */
    public BaseQueryRequestBuilder setTypes(String... types) {
        request.types(types);
        return this;
    }

}

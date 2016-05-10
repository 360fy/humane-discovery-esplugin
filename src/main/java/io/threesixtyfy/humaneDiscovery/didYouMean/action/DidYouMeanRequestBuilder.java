package io.threesixtyfy.humaneDiscovery.didYouMean.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.ElasticsearchClient;

public class DidYouMeanRequestBuilder extends ActionRequestBuilder<DidYouMeanRequest, DidYouMeanResponse, DidYouMeanRequestBuilder> {

    public DidYouMeanRequestBuilder(ElasticsearchClient client, Action<DidYouMeanRequest, DidYouMeanResponse, DidYouMeanRequestBuilder> action) {
        super(client, action, new DidYouMeanRequest());
    }

    /**
     * Sets the indices the search will be executed on.
     */
    public DidYouMeanRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    /**
     * The document types to execute the search against. Defaults to be executed against
     * all types.
     */
    public DidYouMeanRequestBuilder setTypes(String... types) {
        request.types(types);
        return this;
    }

    /**
     * Specifies what type of requested indices to ignore and wildcard indices expressions.
     * <p>
     * For example indices that don't exist.
     */
    public DidYouMeanRequestBuilder setIndicesOptions(IndicesOptions indicesOptions) {
        request().indicesOptions(indicesOptions);
        return this;
    }

    /**
     * Constructs a new search source builder with a raw search query.
     */
    public DidYouMeanRequestBuilder setQuery(String query) {
        request.query(query);
        return this;
    }
}

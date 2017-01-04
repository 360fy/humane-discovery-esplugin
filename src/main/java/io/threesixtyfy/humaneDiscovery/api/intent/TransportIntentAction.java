package io.threesixtyfy.humaneDiscovery.api.intent;

import io.threesixtyfy.humaneDiscovery.api.commons.TransportQueryAction;
import io.threesixtyfy.humaneDiscovery.core.cache.CacheService;
import io.threesixtyfy.humaneDiscovery.core.instance.CarDekhoInstanceContext;
import io.threesixtyfy.humaneDiscovery.core.tagForest.TagForest;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

public class TransportIntentAction extends TransportQueryAction<IntentQuerySource, IntentQueryRequest, IntentResponse> {

    private static final Logger logger = Loggers.getLogger(TransportIntentAction.class);

    @Inject
    public TransportIntentAction(Settings settings,
                                 ThreadPool threadPool,
                                 TransportService transportService,
                                 ActionFilters actionFilters,
                                 IndexNameExpressionResolver indexNameExpressionResolver,
                                 ClusterService clusterService,
                                 IndicesService indicesService,
                                 Client client,
                                 CacheService cacheService) {
        super(settings, IntentAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, clusterService, indicesService, client, IntentQueryRequest::new, cacheService);
    }

    private IntentResponse response(String[] tokens, IntentResult[] intentResults, long startTime) {
        return new IntentResponse("", tokens, intentResults);
    }

    protected IntentResponse response(IntentQueryRequest searchQueryRequest) throws IOException {
        List<TagForest> tagForests = createIntents(searchQueryRequest, new CarDekhoInstanceContext());

        return null;
    }
}

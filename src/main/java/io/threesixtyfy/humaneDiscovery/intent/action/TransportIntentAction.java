package io.threesixtyfy.humaneDiscovery.intent.action;

import io.threesixtyfy.humaneDiscovery.didYouMean.commons.DisjunctsBuilder;
import io.threesixtyfy.humaneDiscovery.didYouMean.commons.SuggestionsBuilder;
import io.threesixtyfy.humaneDiscovery.didYouMean.commons.TokensBuilder;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Arrays;

public class TransportIntentAction extends HandledTransportAction<IntentRequest, IntentResponse> {

    private final ClusterService clusterService;

    private final IndicesService indicesService;

    private final Client client;

    private final SuggestionsBuilder suggestionsBuilder = SuggestionsBuilder.INSTANCE();

    private final DisjunctsBuilder disjunctsBuilder = DisjunctsBuilder.INSTANCE();

    private final TokensBuilder tokensBuilder = TokensBuilder.INSTANCE();

    @Inject
    public TransportIntentAction(Settings settings,
                                 String actionName,
                                 ThreadPool threadPool,
                                 TransportService transportService,
                                 ClusterService clusterService,
                                 IndicesService indicesService,
                                 ActionFilters actionFilters,
                                 IndexNameExpressionResolver indexNameExpressionResolver,
                                 Client client) {
        super(settings, actionName, threadPool, transportService, actionFilters, indexNameExpressionResolver, IntentRequest.class);

        this.clusterService = clusterService;
        this.client = client;
        this.indicesService = indicesService;
    }

    private IntentResponse emptyResponse(long startTime) {
        return new IntentResponse(Math.max(1, System.currentTimeMillis() - startTime));
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void doExecute(IntentRequest intentRequest, ActionListener<IntentResponse> listener) {
        long startTime = System.currentTimeMillis();

        threadPool.executor(ThreadPool.Names.SEARCH).execute(new ActionRunnable<IntentResponse>(listener) {
            @Override
            protected void doRun() throws Exception {
                try {
                    ClusterState clusterState = clusterService.state();

                    String[] inputIndices = indexNameExpressionResolver.concreteIndices(clusterState, intentRequest);

                    // resolve these too
                    String[] intentIndices = indexNameExpressionResolver.concreteIndices(clusterState,
                            intentRequest.indicesOptions(),
                            Arrays.stream(indexNameExpressionResolver.concreteIndices(clusterState, intentRequest)).map(value -> value + ":intent_store").toArray(String[]::new));

                    IndexService indexService = indicesService.indexService(inputIndices[0]);

                    if (indexService == null) {
                        logger.error("IndexService is null for: {}", inputIndices[0]);
                        throw new IOException("IndexService is not found for: " + inputIndices[0]);
                    }

                    // parse with intent entities

                    // List<String> tokens = tokensBuilder.tokens(indexService.analysisService(), intentRequest.querySource().query());

                    listener.onResponse(emptyResponse(startTime));
                } catch (IOException e) {
                    logger.error("failed to execute didYouMean", e);
                    listener.onFailure(e);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                logger.error("failed to execute didYouMean", t);
                super.onFailure(t);
            }
        });
    }

}

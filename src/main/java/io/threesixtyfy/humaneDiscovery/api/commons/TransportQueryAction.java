package io.threesixtyfy.humaneDiscovery.api.commons;

import io.threesixtyfy.humaneDiscovery.core.intent.IntentService;
import io.threesixtyfy.humaneDiscovery.core.tagForest.TagForest;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

public abstract class TransportQueryAction<QS extends QuerySource<QS>, QR extends QueryRequest<QS, QR>, SR extends QueryResponse> extends HandledTransportAction<QR, SR> {

    protected final ClusterService clusterService;
    protected final IndicesService indicesService;
    protected final Client client;

    protected TransportQueryAction(Settings settings,
                                   String actionName,
                                   ThreadPool threadPool,
                                   TransportService transportService,
                                   ActionFilters actionFilters,
                                   IndexNameExpressionResolver indexNameExpressionResolver,
                                   ClusterService clusterService,
                                   IndicesService indicesService,
                                   Client client,
                                   Supplier<QR> request) {
        super(settings, actionName, threadPool, transportService, actionFilters, indexNameExpressionResolver, request);

        this.clusterService = clusterService;
        this.client = client;
        this.indicesService = indicesService;
    }

    protected long tookTime(long startTime) {
        return Math.max(1, System.currentTimeMillis() - startTime);
    }

    protected List<TagForest> createIntents(QR queryRequest) throws IOException {
        return IntentService.INSTANCE().createIntents(queryRequest.instance(),
                queryRequest.querySource().query(),
                clusterService,
                indicesService,
                client,
                indexNameExpressionResolver);
    }

    @SuppressWarnings("unchecked")
    protected void doExecute(QR queryRequest, ActionListener<SR> listener) {
        long startTime = System.currentTimeMillis();

        threadPool.executor(ThreadPool.Names.SEARCH).execute(new ActionRunnable<SR>(listener) {
            @Override
            protected void doRun() throws Exception {
                try {
                    SR sr = response(queryRequest);
                    sr.calculateTookInMillis(startTime);

                    listener.onResponse(sr);
                } catch (IOException e) {
                    logger.error("Failed to execute " + actionName, e);
                    listener.onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception t) {
                logger.error("Failed to execute  " + actionName, t);
                super.onFailure(t);
            }
        });
    }

    protected abstract SR response(QR queryRequest) throws IOException;

}

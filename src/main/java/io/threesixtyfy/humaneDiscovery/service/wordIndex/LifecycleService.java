package io.threesixtyfy.humaneDiscovery.service.wordIndex;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

public class LifecycleService extends AbstractLifecycleComponent {

    private static final Logger logger = Loggers.getLogger(LifecycleService.class);

    private final WordIndexer wordIndexer;
    private final IndexManager indexManager;

    @Inject
    public LifecycleService(final SharedChannel sharedChannel, final Settings settings, final Client client) {
        super(settings);

        this.wordIndexer = new WordIndexer(sharedChannel, client);
        this.indexManager = new IndexManager(sharedChannel, client);
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        logger.info("doStart()");

        indexManager.start();
        wordIndexer.start();
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        logger.info("doStop()");
        shutdown();
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        logger.info("doClose()");
//        shutdown();
    }

    private void shutdown() {
        logger.info("shutting down");

        indexManager.shutdown();
        wordIndexer.shutdown();
    }

}

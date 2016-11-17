package io.threesixtyfy.humaneDiscovery.service.wordIndex;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexEventListener;

public class IndexEventListenerImpl implements IndexEventListener {
    private static final Logger logger = Loggers.getLogger(IndexEventListenerImpl.class);

    private final String key;
    private final SharedChannel sharedChannel;

    public IndexEventListenerImpl(Index index, SharedChannel sharedChannel) {
        this.key = index.getName();
        this.sharedChannel = sharedChannel;
    }

    @Override
    public void afterIndexCreated(final IndexService indexService) {
        if (!indexService.getIndexSettings().getSettings().getAsBoolean(WordIndexConstants.WORD_INDEX_ENABLED_SETTING, false)) {
            return;
        }

        String indexName = indexService.index().getName();

        logger.info("creating word index: {}", indexName);

        sharedChannel.getIndexOperationsQueue().add(new WordIndexCrudRequest(WordIndexCrudRequest.RequestType.CREATE, indexName));
    }

    @Override
    public void afterIndexDeleted(final org.elasticsearch.index.Index index, final Settings indexSettings) {
        if (!indexSettings.getAsBoolean(WordIndexConstants.WORD_INDEX_ENABLED_SETTING, false)) {
            return;
        }

        String indexName = index.getName();

        logger.info("deleting word index: {}", indexName);

        sharedChannel.getIndexOperationsQueue().add(new WordIndexCrudRequest(WordIndexCrudRequest.RequestType.DELETE, indexName));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        IndexEventListenerImpl that = (IndexEventListenerImpl) o;

        return key.equals(that.key);

    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }
}

package io.threesixtyfy.humaneDiscovery.service.wordIndex;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class SharedChannel {

    private final BlockingQueue<WordInfo> wordQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<WordIndexCrudRequest> indexOperationsQueue = new LinkedBlockingQueue<>();

    public BlockingQueue<WordInfo> getWordQueue() {
        return wordQueue;
    }

    public BlockingQueue<WordIndexCrudRequest> getIndexOperationsQueue() {
        return indexOperationsQueue;
    }
}

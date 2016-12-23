package io.threesixtyfy.humaneDiscovery.core.tokenIndex;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class SharedChannel {

    private final BlockingQueue<TokenInfo> tokenQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<TokenIndexCrudRequest> indexOperationsQueue = new LinkedBlockingQueue<>();

    public BlockingQueue<TokenInfo> getTokenQueue() {
        return tokenQueue;
    }

    public BlockingQueue<TokenIndexCrudRequest> getIndexOperationsQueue() {
        return indexOperationsQueue;
    }
}

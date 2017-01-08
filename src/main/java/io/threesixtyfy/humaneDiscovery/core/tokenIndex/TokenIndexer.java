package io.threesixtyfy.humaneDiscovery.core.tokenIndex;

import io.threesixtyfy.humaneDiscovery.core.encoding.EdgeGramEncodingUtils;
import io.threesixtyfy.humaneDiscovery.core.encoding.EncodingUtils;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.TimeValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

import static io.threesixtyfy.humaneDiscovery.core.tokenIndex.TokenIndexConstants.TOKEN_INDEX_TYPE;

public class TokenIndexer extends Thread {

    private static final double EDGE_GRAM_WEIGHT = 0.5f;
    private static final double DEFAULT_WEIGHT = 1.0f;

    private static final String NAME = "token-indexer";
    private static final int HUNDRED_MILLISECOND_IN_NANOS = 1000 * 1000 * 100;
    private static final int MAX_BULK_ACTIONS = 10000;
    private static final int MAX_WORDS_TO_AGGREGATE = 100000;

    private static final Logger logger = Loggers.getLogger(TokenIndexer.class);

    private final SharedChannel sharedChannel;
    private final BulkProcessor bulk;
    private final Client client;

    // we keep edgeGram size as 1
    private final EdgeGramEncodingUtils edgeGramEncodingUtils = new EdgeGramEncodingUtils(1);
    private final EncodingUtils encodingUtils = new EncodingUtils();

    private final ConcurrentHashMap<String, TokenInfo> aggregatedTokenInfo = new ConcurrentHashMap<>();
    private final AtomicInteger aggregatedTokensCount = new AtomicInteger();
    private final AtomicInteger pendingBulkItemCount = new AtomicInteger();
    private final AtomicBoolean stopIndexer = new AtomicBoolean(false);

    public TokenIndexer(SharedChannel sharedChannel, Client client) {
        this.setDaemon(true);
        this.setName(NAME);
        this.sharedChannel = sharedChannel;
        this.client = client;

        this.bulk = buildBulkProcessor(client);
    }

    @Override
    public void run() {
        this.runTokenIndexer();
    }

    public void shutdown() {
        stopIndexer.set(true);

        logger.info("awaiting bulk close");
        try {
            bulk.awaitClose(5, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        logger.info("waiting for {} to exit", NAME);
        try {
            try {
                this.join(1000);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }

        } catch (final IllegalStateException ise) {
            logger.warn("cannot shutdown {} thread", NAME, ise);
        }
    }

    private BulkProcessor buildBulkProcessor(Client client) {
        return BulkProcessor
                .builder(client, new BulkProcessor.Listener() {

                    @Override
                    public void beforeBulk(final long executionId, final BulkRequest request) {
                    }

                    @Override
                    // failure of bulk requests
                    public void afterBulk(final long executionId, final BulkRequest request, final Throwable failure) {
                        logger.error("Bulk error", failure);
                        pendingBulkItemCount.addAndGet(-request.numberOfActions());
                    }

                    @Override
                    // success of bulk requests
                    public void afterBulk(final long executionId, final BulkRequest request, final BulkResponse response) {
                        pendingBulkItemCount.addAndGet(-response.getItems().length);
                    }
                })
                .setBulkActions(MAX_BULK_ACTIONS)
                .setConcurrentRequests(0)
                .setFlushInterval(TimeValue.timeValueSeconds(30))
                .build();
    }

    private void consumeAggregates() {
        // if 10K words have been accumulated
        int count = this.aggregatedTokensCount.get();
        if (count == 0) {
            return;
        }

        logger.info("submitting aggregates to bulk processor: {}", count);
        // create index requests
        for (Map.Entry<String, TokenInfo> entry : this.aggregatedTokenInfo.entrySet()) {
            TokenInfo tokenInfo = entry.getValue();

            bulk.add(new IndexRequest(tokenInfo.getIndexName(), TOKEN_INDEX_TYPE, tokenInfo.getKey()).source(tokenInfo.map()));
            pendingBulkItemCount.addAndGet(1);
        }

        // remove all words
        this.aggregatedTokensCount.set(0);
        this.aggregatedTokenInfo.clear();

        logger.info("flushing bulk, count: {}", pendingBulkItemCount.get());

        awaitBulkComplete();
    }

    private void runTokenIndexer() {
        while (!Thread.interrupted() && !stopIndexer.get()) {
            try {
                TokenInfo tokenInfo = sharedChannel.getTokenQueue().poll(10, TimeUnit.SECONDS);

                if (tokenInfo != null) {
                    aggregate(tokenInfo);
                }

                if (this.aggregatedTokensCount.get() > MAX_WORDS_TO_AGGREGATE || tokenInfo == null && this.aggregatedTokensCount.get() > 0) {
                    consumeAggregates();
                }
            } catch (InterruptedException e) {
                // consume whatever is available
                consumeAggregates();
            }
        }

        // we consume anything remaining
        consumeAggregates();
        awaitBulkComplete();
    }

    private String tokenInfoKey(String indexName, String key) {
        return indexName + "/" + key;
    }

    private void aggregate(TokenInfo toAggregate) {
        final String indexName = toAggregate.getIndexName();
        final String tokenInfoKey = tokenInfoKey(indexName, toAggregate.getKey());

        TokenInfo existingTokenInfo = this.aggregatedTokenInfo.get(tokenInfoKey);
        if (existingTokenInfo == null) {
            existingTokenInfo = fetch(indexName, toAggregate.getKey());
        }

        if (existingTokenInfo == null) {
            this.aggregatedTokenInfo.put(tokenInfoKey, buildEncodings(toAggregate));
            this.aggregatedTokensCount.addAndGet(1);
        } else {
            // aggregate with existing
            existingTokenInfo.aggregate(toAggregate);
        }
    }

    private Map<String, List<String>> convert(Map<String, Set<String>> encodings) {
        Map<String, List<String>> result = new HashMap<>();

        encodings
                .entrySet()
                .forEach(e -> result.put(e.getKey(), e.getValue().stream().collect(Collectors.toList())));

        return result;
    }

    private TokenInfo fetch(String indexName, String key) {
        GetRequest getRequest = new GetRequest(indexName, TOKEN_INDEX_TYPE, key);
        Map<String, Object> getResponse = client.get(getRequest).actionGet().getSourceAsMap();
        if (getResponse != null) {
            TokenInfo tokenInfo = TokenInfo.unmap(indexName, getResponse);
            this.aggregatedTokenInfo.put(tokenInfoKey(indexName, key), tokenInfo);
            this.aggregatedTokensCount.addAndGet(1);

            return tokenInfo;
        }

        return null;
    }

    private TokenInfo buildEncodings(TokenInfo tokenInfo) {
        List<TokenEncoding> tokenEncodings = new ArrayList<>();

        StringBuilder sb = null;

        int size = tokenInfo.getTokens().size();

        for (String token : tokenInfo.getTokens()) {
            buildEncodings(token, tokenEncodings, TokenEncoding.TokenType.Full, DEFAULT_WEIGHT);

            // also build edgeGram
            for (String edgeGramToken : edgeGramEncodingUtils.buildEncodings(token)) {
                buildEncodings(edgeGramToken, tokenEncodings, TokenEncoding.TokenType.EdgeGram, EDGE_GRAM_WEIGHT);
            }

            if (size > 1) {
                if (sb == null) {
                    sb = new StringBuilder();
                }

                sb.append(token);
            }
        }

        if (size > 1 && sb != null) {
            String token = sb.toString();
            buildEncodings(token, tokenEncodings, TokenEncoding.TokenType.Joined, /*size * */DEFAULT_WEIGHT);
        }

        tokenInfo.setEncodings(tokenEncodings);

        return tokenInfo;
    }

    private void buildEncodings(String token, List<TokenEncoding> tokenEncodings, TokenEncoding.TokenType tokenType, double weight) {
        Map<String, Set<String>> encodings = encodingUtils.buildEncodings(token, false);

        tokenEncodings.add(new TokenEncoding(token, convert(encodings), tokenType, weight));
    }

    private void awaitBulkComplete() {
        try {
            bulk.flush();

            int count;
            while ((count = pendingBulkItemCount.get()) > 0) {
                logger.info("waiting bulk to get complete, count: {}", count);
                LockSupport.parkNanos(HUNDRED_MILLISECOND_IN_NANOS);
            }
        } catch (IllegalStateException ise) {
            logger.warn("Bulk is already closed");
        }
    }
}

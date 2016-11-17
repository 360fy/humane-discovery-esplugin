package io.threesixtyfy.humaneDiscovery.service.wordIndex;

import io.threesixtyfy.humaneDiscovery.core.encoding.PhoneticEncodingUtils;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.TimeValue;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static io.threesixtyfy.humaneDiscovery.service.wordIndex.WordIndexConstants.BIGRAM_WORD_INDEX_TYPE;
import static io.threesixtyfy.humaneDiscovery.service.wordIndex.WordIndexConstants.UNIGRAM_WORD_INDEX_TYPE;

public class WordIndexer extends Thread {

    private static final String NAME = "word-indexer";
    private static final int HUNDRED_MILLISECOND_IN_NANOS = 1000 * 1000 * 100;
    private static final int MAX_BULK_ACTIONS = 10000;
    private static final int MAX_WORDS_TO_AGGREGATE = 100000;

    private static final Logger logger = Loggers.getLogger(WordIndexer.class);

    private final SharedChannel sharedChannel;
    private final BulkProcessor bulk;
    private final Client client;

    private final ConcurrentHashMap<String, WordAggregateInfo> wordsAggregateInfo = new ConcurrentHashMap<>();
    private final AtomicInteger aggregatedWordsCount = new AtomicInteger();
    private final AtomicInteger pendingBulkItemCount = new AtomicInteger();
    private final AtomicBoolean stopIndexer = new AtomicBoolean(false);

    public WordIndexer(SharedChannel sharedChannel, Client client) {
        this.setDaemon(true);
        this.setName(NAME);
        this.sharedChannel = sharedChannel;
        this.client = client;

        this.bulk = buildBulkProcessor(client);
    }

    @Override
    public void run() {
        this.runWordIndexer();
    }

    public void shutdown() {
        stopIndexer.set(true);

        logger.info("awaiting bulk close");
        try {
            bulk.awaitClose(5, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        logger.info("waiting for wordIndexer to exit");
        try {
            try {
                this.join(1000);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }

        } catch (final IllegalStateException ise) {
            logger.warn("cannot shutdown wordIndexer thread", ise);
        }
    }

    private BulkProcessor buildBulkProcessor(Client client) {
        return BulkProcessor.builder(client, new BulkProcessor.Listener() {

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
        int count = this.aggregatedWordsCount.get();
        if (count == 0) {
            return;
        }

        logger.info("submitting aggregates to bulk processor: {}", count);
        // create index requests
        for (Map.Entry<String, WordAggregateInfo> entry : this.wordsAggregateInfo.entrySet()) {
            WordAggregateInfo wordAggregateInfo = entry.getValue();

            if (wordAggregateInfo instanceof BigramWordAggregateInfo) {
                BigramWordAggregateInfo bigramWordAggregateInfo = (BigramWordAggregateInfo) wordAggregateInfo;
                bulk.add(new IndexRequest(wordAggregateInfo.getIndexName(), BIGRAM_WORD_INDEX_TYPE, bigramWordAggregateInfo.getWord()).source(bigramWordAggregateInfo.map()));
            } else {
                bulk.add(new IndexRequest(wordAggregateInfo.getIndexName(), UNIGRAM_WORD_INDEX_TYPE, wordAggregateInfo.getWord()).source(wordAggregateInfo.map()));
            }
            pendingBulkItemCount.addAndGet(1);
        }

        // remove all words
        this.aggregatedWordsCount.set(0);
        this.wordsAggregateInfo.clear();

        logger.info("flushing bulk, count: {}", pendingBulkItemCount.get());

        awaitBulkComplete();
    }

    private void buildFieldAggregateInfo(Map<String, FieldAggregateInfo> subAggregatesInfo, String field, double weight, boolean edgeGram) {
        if (!subAggregatesInfo.containsKey(field)) {
            subAggregatesInfo.put(field, new FieldAggregateInfo(field, weight, 1, edgeGram ? 0 : 1, edgeGram ? 1 : 0));
        } else {
            FieldAggregateInfo fieldAggregateInfo = subAggregatesInfo.get(field);
            fieldAggregateInfo.addWeight(weight);
            fieldAggregateInfo.incrementTotalCount();
            if (edgeGram) {
                fieldAggregateInfo.incrementCountAsEdgeGram();
            } else {
                fieldAggregateInfo.incrementCountAsFullWord();
            }
        }
    }

    private void runWordIndexer() {
        PhoneticEncodingUtils phoneticEncodingUtils = new PhoneticEncodingUtils();

        while (!Thread.interrupted() && !stopIndexer.get()) {
            try {
                WordInfo wordInfo = sharedChannel.getWordQueue().poll(10, TimeUnit.SECONDS);

                if (wordInfo != null) {
                    aggregate(phoneticEncodingUtils, wordInfo);
                }

                if (this.aggregatedWordsCount.get() > MAX_WORDS_TO_AGGREGATE || wordInfo == null && this.aggregatedWordsCount.get() > 0) {
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

    private void aggregate(PhoneticEncodingUtils phoneticEncodingUtils, WordInfo wordInfo) {
        String indexName = wordInfo.getIndexName();
        String word = wordInfo.getWord();
        String word1 = null;
        String word2 = null;
        String wordKey;

        boolean bigram = wordInfo instanceof BigramWordInfo;

        if (bigram) {
            word1 = ((BigramWordInfo) wordInfo).getWord1();
            word2 = ((BigramWordInfo) wordInfo).getWord2();
            wordKey = indexName + WordIndexConstants.BI + word;
        } else {
            wordKey = indexName + WordIndexConstants.UNI + word;
        }

        WordAggregateInfo wordAggregateInfo;
        if (!this.wordsAggregateInfo.containsKey(wordKey)) {
            wordAggregateInfo = buildWordAggregate(phoneticEncodingUtils, indexName, word, word1, word2, bigram);

            this.wordsAggregateInfo.put(wordKey, wordAggregateInfo);
            this.aggregatedWordsCount.addAndGet(1);
        } else {
            wordAggregateInfo = this.wordsAggregateInfo.get(wordKey);
        }

        // >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
        // this is common between unigram and bigram
        boolean edgeGram = wordInfo.isEdgeGram();

        wordAggregateInfo.addWeight(wordInfo.getSuggestionWeight());
        wordAggregateInfo.incrementCount();
        if (edgeGram) {
            wordAggregateInfo.incrementCountAsEdgeGram();
        } else {
            wordAggregateInfo.incrementCountAsFullWord();
        }

        String field = wordInfo.getFieldName();

        String originalWord = wordInfo.getOriginalWord();
        if (originalWord != null) {
            OriginalWordInfo originalWordInfo;
            if (wordAggregateInfo.getOriginalWords().containsKey(originalWord)) {
                originalWordInfo = wordAggregateInfo.getOriginalWords().get(originalWord);
                originalWordInfo.incrementCount();
                originalWordInfo.addWeight(wordInfo.getSuggestionWeight());
            } else {
                originalWordInfo = new OriginalWordInfo(originalWord, wordInfo.getOriginalDisplay(), wordInfo.getSuggestionWeight(), 1);
                wordAggregateInfo.getOriginalWords().put(originalWord, originalWordInfo);
            }

            buildFieldAggregateInfo(originalWordInfo.getFieldAggregateInfo(), field, wordInfo.getSuggestionWeight(), edgeGram);
        }

        // aggregate by fieldName
        buildFieldAggregateInfo(wordAggregateInfo.getFieldAggregateInfo(), field, wordInfo.getSuggestionWeight(), edgeGram);
        // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
    }

    private WordAggregateInfo buildWordAggregate(PhoneticEncodingUtils phoneticEncodingUtils, String indexName, String word, String word1, String word2, boolean bigram) {
        WordAggregateInfo wordAggregateInfo;
        if (bigram) {
            GetRequest getRequest = new GetRequest(indexName, BIGRAM_WORD_INDEX_TYPE, word);
            Map<String, Object> getResponse = client.get(getRequest).actionGet().getSourceAsMap();
            if (getResponse == null) {
                BigramWordAggregateInfo bigramWordAggregateInfo = new BigramWordAggregateInfo(indexName, word1, word2);

                // do it for word 1 phonetic encodings
                phoneticEncodingUtils.buildEncodings(word, bigramWordAggregateInfo.getEncodings(), false);

                // do it for word 1 phonetic encodings
                phoneticEncodingUtils.buildEncodings(word1, bigramWordAggregateInfo.getWord1Encodings(), false);

                // do it for word2 phonetic encodings
                phoneticEncodingUtils.buildEncodings(word2, bigramWordAggregateInfo.getWord2Encodings(), false);

                wordAggregateInfo = bigramWordAggregateInfo;
            } else {
                wordAggregateInfo = BigramWordAggregateInfo.unmap(indexName, getResponse);
            }
        } else {
            GetRequest getRequest = new GetRequest(indexName, UNIGRAM_WORD_INDEX_TYPE, word);
            Map<String, Object> getResponse = client.get(getRequest).actionGet().getSourceAsMap();
            if (getResponse == null) {
                wordAggregateInfo = new WordAggregateInfo(indexName, word);
                phoneticEncodingUtils.buildEncodings(word, wordAggregateInfo.getEncodings(), false);
            } else {
                wordAggregateInfo = WordAggregateInfo.unmap(indexName, getResponse);
            }
        }
        return wordAggregateInfo;
    }

    private void awaitBulkComplete() {
        bulk.flush();

        int count;
        while ((count = pendingBulkItemCount.get()) > 0) {
            logger.info("waiting bulk to get complete, count: {}", count);
            LockSupport.parkNanos(HUNDRED_MILLISECOND_IN_NANOS);
        }
    }
}

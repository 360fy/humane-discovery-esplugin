package io.threesixtyfy.humaneDiscovery.didYouMean.builder;

import io.threesixtyfy.humaneDiscovery.didYouMean.commons.EdgeGramEncodingUtils;
import io.threesixtyfy.humaneDiscovery.didYouMean.commons.PhoneticEncodingUtils;
import io.threesixtyfy.humaneDiscovery.mapper.HumaneDescriptiveTextFieldMapper;
import io.threesixtyfy.humaneDiscovery.mapper.HumaneTextFieldMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.indexing.IndexingOperationListener;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesLifecycle;
import org.elasticsearch.indices.IndicesService;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

import static io.threesixtyfy.humaneDiscovery.didYouMean.commons.Constants.BIGRAM_DID_YOU_MEAN_INDEX_TYPE;
import static io.threesixtyfy.humaneDiscovery.didYouMean.commons.Constants.UNIGRAM_DID_YOU_MEAN_INDEX_TYPE;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;

public class DidYouMeanBuilderService extends AbstractLifecycleComponent<DidYouMeanBuilderService> {

    public static final String DID_YOY_MEAN_ENABLE_SETTING = "index.did_you_mean_enabled";

    private final IndicesService indicesService;
    private final Client client;
    private final ClusterService clusterService;
    private volatile boolean isMaster = false;
    private final BulkProcessor bulk;

    private final AtomicInteger pendingBulkItemCount = new AtomicInteger();

    private final HashSet<String> currentIndexOperations = new HashSet<>();

    private final ConcurrentLinkedQueue<WordInfo> wordQueue = new ConcurrentLinkedQueue<>();
    private final AtomicInteger queuedWordCount = new AtomicInteger();

    private final ConcurrentHashMap<String, WordAggregateInfo> wordsAggregateInfo = new ConcurrentHashMap<>();
    private final AtomicInteger aggregatedWordsCount = new AtomicInteger();

    private final AtomicBoolean stopPoller = new AtomicBoolean(false);
    private final AtomicBoolean pollerParked = new AtomicBoolean(false);
    private Thread poller;

    private final IndicesLifecycle.Listener indicesLifecycleListener = new IndicesLifecycle.Listener() {

        private final ConcurrentHashMap<ShardId, WatchIndexOpListener> listeners = new ConcurrentHashMap<>();

        @Override
        public void afterIndexCreated(final IndexService indexService) {
            boolean didYouMeanEnabled = indexService.indexSettings().getAsBoolean(DID_YOY_MEAN_ENABLE_SETTING, false);

            if (!isMaster || !didYouMeanEnabled) {
                return;
            }

            String indexName = indexService.index().name();

            logger.info("creating did you mean index: {}", indexName);

            createDidYouMeanIndex(indexName);
        }

        @Override
        public void afterIndexDeleted(final org.elasticsearch.index.Index index, final Settings indexSettings) {
            boolean didYouMeanEnabled = indexSettings.getAsBoolean(DID_YOY_MEAN_ENABLE_SETTING, false);

            if (!isMaster || !didYouMeanEnabled) {
                return;
            }

            String indexName = index.name();

            logger.info("deleting did you mean index: {}", indexName);

            deleteDidYouMeanIndex(indexName);
        }

        @Override
        public void afterIndexShardStarted(final IndexShard indexShard) {
            boolean didYouMeanEnabled = indexShard.indexSettings().getAsBoolean("index.did_you_mean_enabled", false);

            if (!indexShard.routingEntry().primary() || !didYouMeanEnabled) {
                return;
            }

            String indexName = indexShard.indexService().index().name();

            logger.info("scheduling listeners for {}", indexName);

            final WatchIndexOpListener auditListener = new WatchIndexOpListener(didYouMeanIndexName(indexName), indexShard.indexService().analysisService());

            indexShard.indexingService().addListener(auditListener);

            listeners.put(indexShard.shardId(), auditListener);

            logger.info("listener for shard {} added", indexShard.shardId());
        }

        @Override
        public void beforeIndexShardClosed(final ShardId shardId, final IndexShard indexShard, final Settings indexSettings) {
            final WatchIndexOpListener listener = listeners.remove(shardId);

            if (listener != null) {
                indexShard.indexingService().removeListener(listener);
                logger.info("listener for shard {} removed", shardId);
            }
        }
    };

    @Inject
    public DidYouMeanBuilderService(final Settings settings, final IndicesService indicesService, final Client client, final ClusterService clusterService) {
        super(settings);

        this.indicesService = indicesService;

        this.client = client;
        this.clusterService = clusterService;
        bulk = BulkProcessor.builder(client, new BulkProcessor.Listener() {

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
        }).setBulkActions(10000).setConcurrentRequests(0)
                //.setBulkSize()
                .setFlushInterval(TimeValue.timeValueSeconds(30))
                .build();
    }

    private String didYouMeanIndexName(String indexName) {
        return indexName + ":did_you_mean_store";
    }

    private void createDidYouMeanIndex(final String indexName) {
        synchronized (currentIndexOperations) {
            if (currentIndexOperations.contains(indexName)) {
                return;
            }

            currentIndexOperations.add(indexName);
        }

        final String didYouMeanIndexName = didYouMeanIndexName(indexName);

        client.admin().indices().prepareExists(didYouMeanIndexName).execute(new ActionListener<IndicesExistsResponse>() {
            @Override
            public void onResponse(final IndicesExistsResponse response) {
                if (response.isExists()) {

                    synchronized (currentIndexOperations) {
                        currentIndexOperations.remove(indexName);
                    }

                    if (logger.isDebugEnabled()) {
                        logger.debug("{} index exists already.", didYouMeanIndexName);
                    }
                } else {
                    logger.info("will create {} index", didYouMeanIndexName);

                    final Settings indexSettings = Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0).build();

                    client.admin()
                            .indices()
                            .prepareCreate(didYouMeanIndexName)
                            .addMapping(UNIGRAM_DID_YOU_MEAN_INDEX_TYPE, getUnigramMapping())
                            .addMapping(BIGRAM_DID_YOU_MEAN_INDEX_TYPE, getBigramMapping())
                            .setSettings(indexSettings)
                            .execute(new ActionListener<CreateIndexResponse>() {
                                @Override
                                public void onResponse(final CreateIndexResponse response) {
                                    synchronized (currentIndexOperations) {
                                        currentIndexOperations.remove(indexName);
                                    }

                                    if (!response.isAcknowledged()) {
                                        logger.error("failed to create {}.", didYouMeanIndexName);
                                        throw new ElasticsearchException("Failed to create index " + didYouMeanIndexName);
                                    } else {
                                        logger.info("successfully created {}.", didYouMeanIndexName);
                                    }
                                }

                                @Override
                                public void onFailure(final Throwable e) {
                                    synchronized (currentIndexOperations) {
                                        currentIndexOperations.remove(indexName);
                                    }

                                    logger.error("failed to create {}", e, didYouMeanIndexName);
                                }
                            });
                }
            }

            @Override
            public void onFailure(final Throwable e) {
                synchronized (currentIndexOperations) {
                    currentIndexOperations.remove(indexName);
                }
                logger.error("The state of {} index is invalid.", e, didYouMeanIndexName);
            }
        });
    }

    private void deleteDidYouMeanIndex(final String indexName) {
        synchronized (currentIndexOperations) {
            if (currentIndexOperations.contains(indexName)) {
                return;
            }

            currentIndexOperations.add(indexName);
        }

        final String didYouMeanIndexName = didYouMeanIndexName(indexName);

        client.admin().indices().prepareExists(didYouMeanIndexName).execute(new ActionListener<IndicesExistsResponse>() {
            @Override
            public void onResponse(final IndicesExistsResponse response) {
                if (response.isExists()) {
                    logger.info("will delete {} index", didYouMeanIndexName);

                    client.admin().indices().prepareDelete(didYouMeanIndexName).execute(new ActionListener<DeleteIndexResponse>() {
                        @Override
                        public void onResponse(final DeleteIndexResponse response) {
                            synchronized (currentIndexOperations) {
                                currentIndexOperations.remove(indexName);
                            }

                            if (!response.isAcknowledged()) {
                                logger.error("failed to delete {}.", didYouMeanIndexName);
                                throw new ElasticsearchException("Failed to create index " + didYouMeanIndexName);
                            } else {
                                logger.info("successfully deleted {}.", didYouMeanIndexName);
                            }
                        }

                        @Override
                        public void onFailure(final Throwable e) {
                            synchronized (currentIndexOperations) {
                                currentIndexOperations.remove(indexName);
                            }

                            logger.error("failed to delete {}", e, didYouMeanIndexName);
                        }
                    });
                } else {
                    synchronized (currentIndexOperations) {
                        currentIndexOperations.remove(indexName);
                    }

                    if (logger.isDebugEnabled()) {
                        logger.debug("{} index does not exist.", didYouMeanIndexName);
                    }
                }
            }

            @Override
            public void onFailure(final Throwable e) {
                synchronized (currentIndexOperations) {
                    currentIndexOperations.remove(indexName);
                }

                logger.error("The state of {} index is invalid.", e, didYouMeanIndexName);
            }
        });
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        logger.info("doStart()");

        newPollerThread();

        this.clusterService.add(event -> {
            isMaster = event.localNodeMaster();
        });

        this.indicesService.indicesLifecycle().addListener(indicesLifecycleListener);
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        logger.info("doStop()");
        shutdown();
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        logger.info("doClose()");
        shutdown();
    }

    private void consumeAggregates(boolean force) {
        // if 10K words have been accumulated
        int count = aggregatedWordsCount.get();
        if (count >= 10000 || ((force || stopPoller.get()) && count > 0)) {
            logger.info("submitting aggregates to bulk processor: {}", count);
            // create index requests
            for (Map.Entry<String, WordAggregateInfo> entry : wordsAggregateInfo.entrySet()) {
                WordAggregateInfo wordAggregateInfo = entry.getValue();

//                logger.info("submitting bulk request for index: {}, obj: {}", wordAggregateInfo.indexName, wordAggregateInfo);

                if (wordAggregateInfo instanceof BigramWordAggregateInfo) {
                    BigramWordAggregateInfo bigramWordAggregateInfo = (BigramWordAggregateInfo) wordAggregateInfo;
                    bulk.add(new IndexRequest(wordAggregateInfo.indexName, BIGRAM_DID_YOU_MEAN_INDEX_TYPE, bigramWordAggregateInfo.word).source(bigramWordAggregateInfo.map()));
                } else {
                    bulk.add(new IndexRequest(wordAggregateInfo.indexName, UNIGRAM_DID_YOU_MEAN_INDEX_TYPE, wordAggregateInfo.word).source(wordAggregateInfo.map()));
                }
                pendingBulkItemCount.addAndGet(1);
            }

            // remove all words
            aggregatedWordsCount.set(0);
            wordsAggregateInfo.clear();

            logger.info("flushing bulk, count: {}", pendingBulkItemCount.get());

            awaitBulkComplete();
        }
    }

    private void subAggregateWordInfo(Map<String, SubAggregateInfo> subAggregatesInfo, String field, double weight, boolean edgeGram) {
        if (!subAggregatesInfo.containsKey(field)) {
            subAggregatesInfo.put(field, new SubAggregateInfo(field, weight, 1, edgeGram ? 0 : 1, edgeGram ? 1 : 0));
        } else {
            SubAggregateInfo subAggregateInfo = subAggregatesInfo.get(field);
            subAggregateInfo.totalWeight += weight;
            subAggregateInfo.totalCount++;
            if (edgeGram) {
                subAggregateInfo.countAsEdgeGram++;
            } else {
                subAggregateInfo.countAsFullWord++;
            }
        }
    }

    private void newPollerThread() {
        poller = new Thread(() -> {
            PhoneticEncodingUtils phoneticEncodingUtils = new PhoneticEncodingUtils();

            int idleWaitCount = 0;
            while (true) {
                consumeAggregates(idleWaitCount >= 5);

                if (queuedWordCount.get() == 0) {
                    // if stopped then return
                    if (stopPoller.get()) {
                        // whatever words that may have been added, consume them
                        consumeAggregates(true);
                        return;
                    }

                    pollerParked.set(true);
                    // there are aggregates to flush
                    if (aggregatedWordsCount.get() > 0) {
//                        logger.info("Parked poller: {}", idleWaitCount);
                        idleWaitCount++;
                        LockSupport.parkNanos(poller, 1000000000L); // wait for 1 second
                        pollerParked.set(false);
                    } else {
                        idleWaitCount = 0;
                        logger.info("parking poller for long...");
                        LockSupport.park(poller);
                    }

                    continue;
                }

                idleWaitCount = 0;

                WordInfo wordInfo = wordQueue.poll();

                queuedWordCount.addAndGet(-1);

                String indexName = wordInfo.indexName;
                String word = wordInfo.word;
                String word1 = null;
                String word2 = null;
                String wordKey = null;

                boolean bigram = wordInfo instanceof BigramWordInfo;

                if (bigram) {
                    word1 = ((BigramWordInfo) wordInfo).word1;
                    word2 = ((BigramWordInfo) wordInfo).word2;
                    wordKey = indexName + "/bi/" + word;
                } else {
                    wordKey = indexName + "/uni/" + word;
                }

//                logger.info("Aggregating {} for index: {}", wordKey, indexName);

                WordAggregateInfo wordAggregateInfo = null;
                if (!wordsAggregateInfo.containsKey(wordKey)) {
                    if (bigram) {
                        GetRequest getRequest = new GetRequest(indexName, BIGRAM_DID_YOU_MEAN_INDEX_TYPE, word);
                        Map<String, Object> getResponse = client.get(getRequest).actionGet().getSourceAsMap();
                        if (getResponse == null) {
                            BigramWordAggregateInfo bigramWordAggregateInfo = new BigramWordAggregateInfo(indexName, word1, word2);

                            // do it for word 1 phonetic encodings
                            phoneticEncodingUtils.buildEncodings(word, bigramWordAggregateInfo.encodings, false);

                            // do it for word 1 phonetic encodings
                            phoneticEncodingUtils.buildEncodings(word1, bigramWordAggregateInfo.word1Encodings, false);

                            // do it for word2 phonetic encodings
                            phoneticEncodingUtils.buildEncodings(word2, bigramWordAggregateInfo.word2Encodings, false);

                            wordAggregateInfo = bigramWordAggregateInfo;
                        } else {
                            wordAggregateInfo = BigramWordAggregateInfo.unmap(indexName, getResponse);
                        }
                    } else {
                        GetRequest getRequest = new GetRequest(indexName, UNIGRAM_DID_YOU_MEAN_INDEX_TYPE, word);
                        Map<String, Object> getResponse = client.get(getRequest).actionGet().getSourceAsMap();
                        if (getResponse == null) {
                            wordAggregateInfo = new WordAggregateInfo(indexName, word);
                            phoneticEncodingUtils.buildEncodings(word, wordAggregateInfo.encodings, false);
                        } else {
                            wordAggregateInfo = WordAggregateInfo.unmap(indexName, getResponse);
                        }
                    }

                    wordsAggregateInfo.put(wordKey, wordAggregateInfo);
                    aggregatedWordsCount.addAndGet(1);
                } else {
                    wordAggregateInfo = wordsAggregateInfo.get(wordKey);
                }

                // >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
                // this is common between unigram and bigram
                boolean edgeGram = wordInfo.edgeGram;

                wordAggregateInfo.totalWeight += wordInfo.suggestionWeight;
                wordAggregateInfo.totalCount++;
                if (edgeGram) {
                    wordAggregateInfo.countAsEdgeGram++;
                } else {
                    wordAggregateInfo.countAsFullWord++;
                }

                String originalWord = wordInfo.originalWord;
                if (originalWord != null) {
                    if (wordAggregateInfo.originalWords.containsKey(originalWord)) {
                        OriginalWordInfo originalWordInfo = wordAggregateInfo.originalWords.get(originalWord);
                        originalWordInfo.totalCount++;
                        originalWordInfo.totalWeight += wordInfo.suggestionWeight;
                    } else {
                        wordAggregateInfo.originalWords.put(originalWord, new OriginalWordInfo(originalWord, wordInfo.originalDisplay, wordInfo.suggestionWeight, 1));
                    }
                }

                String type = wordInfo.typeName;

                // aggregate by type
                subAggregateWordInfo(wordAggregateInfo.typeAggregateInfo, type, wordInfo.suggestionWeight, edgeGram);

                String field = type + ":" + wordInfo.field;

                // aggregate by field
                subAggregateWordInfo(wordAggregateInfo.fieldAggregateInfo, field, wordInfo.suggestionWeight, edgeGram);
                // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
            }
        });
        poller.setDaemon(true);
        poller.setName("poller");
        poller.start();
    }

    private void shutdown() {
        logger.info("shutting down");
        stopPoller.set(true);
        flush();

        this.indicesService.indicesLifecycle().removeListener(indicesLifecycleListener);

        logger.info("awaiting bulk close");
        try {
            bulk.awaitClose(5, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        logger.info("waiting for poller to exit");
        try {
            try {
                if (poller != null) {
                    poller.join(1000);
                }
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }

        } catch (final IllegalStateException ise) {
            logger.warn("cannot shutdown poller thread", ise);
        }
    }

    public void flush() {
        logger.info("flushing");
        if (pollerParked.compareAndSet(true, false)) {
            logger.info("un-parking poller for flush");
            LockSupport.unpark(poller);
        }

        awaitQueuedWordsConsume();
        awaitAggregateWordsConsume();
        awaitBulkComplete();
    }

    private void awaitQueuedWordsConsume() {
        int count;
        do {
            count = queuedWordCount.get();
            logger.info("awaiting queued words consume, count: {}", count);
            if (count == 0) {
                return;
            }

            LockSupport.parkNanos(1000 * 1000 * 100);
        } while (count > 0);
    }

    private void awaitAggregateWordsConsume() {
        int count;
        do {
            count = aggregatedWordsCount.get();
            logger.info("awaiting aggregate words consume, count: {}", count);
            if (count == 0) {
                return;
            }

            LockSupport.parkNanos(1000 * 1000 * 100);
        } while (count > 0);
    }

    private void awaitBulkComplete() {
        bulk.flush();

        int count;
        while ((count = pendingBulkItemCount.get()) > 0) {
            logger.info("waiting bulk to get complete, count: {}", count);
            LockSupport.parkNanos(1000 * 1000 * 100);
        }
    }

    private XContentBuilder getUnigramMapping() {
        try {
            return XContentBuilder.builder(JsonXContent.jsonXContent).startObject().startObject(UNIGRAM_DID_YOU_MEAN_INDEX_TYPE)
                    .startObject("properties")

                    // word
                    .startObject("word").field("type", "string").field("index", "not_analyzed").endObject()

                    // encodings
                    .startObject("encodings").field("type", "string").field("index", "not_analyzed").endObject()

                    // weight
                    .startObject("totalWeight").field("type", "double").field("index", "no").endObject()

                    // totalCount
                    .startObject("totalCount").field("type", "long").field("index", "no").endObject()

                    // countAsFullWord
                    .startObject("countAsFullWord").field("type", "long").field("index", "no").endObject()

                    // countAsEdgeGram
                    .startObject("countAsEdgeGram").field("type", "long").field("index", "no").endObject()

                    // field stats start
                    // >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
                    .startObject("fieldStats")

                    .field("type", "nested")

                    .startObject("properties")

                    // name
                    .startObject("name").field("type", "string").field("index", "not_analyzed").endObject()

                    // totalWeight
                    .startObject("totalWeight").field("type", "double").field("index", "no").endObject()

                    // totalCount
                    .startObject("totalCount").field("type", "long").field("index", "no").endObject()

                    // countAsFullWord
                    .startObject("countAsFullWord").field("type", "long").field("index", "no").endObject()

                    // countAsEdgeGram
                    .startObject("countAsEdgeGram").field("type", "long").field("index", "no").endObject()

                    .endObject().endObject()
                    // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<


                    // type stats start
                    // >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
                    .startObject("typeStats")

                    .field("type", "nested")

                    .startObject("properties")

                    // name
                    .startObject("name").field("type", "string").field("index", "not_analyzed").endObject()

                    // totalWeight
                    .startObject("totalWeight").field("type", "double").field("index", "no").endObject()

                    // totalCount
                    .startObject("totalCount").field("type", "long").field("index", "no").endObject()

                    // countAsFullWord
                    .startObject("countAsFullWord").field("type", "long").field("index", "no").endObject()

                    // countAsEdgeGram
                    .startObject("countAsEdgeGram").field("type", "long").field("index", "no").endObject()

                    .endObject().endObject()
                    // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

                    // original word stats start
                    // >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
                    .startObject("originalWords")

                    .field("type", "nested")

                    .startObject("properties")

                    // word
                    .startObject("word").field("type", "string").field("index", "no").endObject()

                    // display
                    .startObject("display").field("type", "string").field("index", "no").endObject()

                    // totalWeight
                    .startObject("totalWeight").field("type", "double").field("index", "no").endObject()

                    // totalCount
                    .startObject("totalCount").field("type", "long").field("index", "no").endObject()

                    .endObject().endObject()
                    // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

                    .endObject().endObject().endObject()

                    ;
        } catch (final IOException e) {
            logger.error("IOException in building mapping", e);
            return null;
        }
    }

    private XContentBuilder getBigramMapping() {
        try {
            return XContentBuilder.builder(JsonXContent.jsonXContent).startObject().startObject(BIGRAM_DID_YOU_MEAN_INDEX_TYPE)
                    .startObject("properties")

                    // word
                    .startObject("word").field("type", "string").field("index", "not_analyzed").endObject()

                    // word1
                    .startObject("word1").field("type", "string").field("index", "not_analyzed").endObject()

                    // word2
                    .startObject("word2").field("type", "string").field("index", "not_analyzed").endObject()

                    // encodings
                    .startObject("encodings").field("type", "string").field("index", "not_analyzed").endObject()

                    // word1 encodings
                    .startObject("word1Encodings").field("type", "string").field("index", "not_analyzed").endObject()

                    // word2 encodings
                    .startObject("word2Encodings").field("type", "string").field("index", "not_analyzed").endObject()

                    // weight
                    .startObject("totalWeight").field("type", "double").field("index", "no").endObject()

                    // totalCount
                    .startObject("totalCount").field("type", "long").field("index", "no").endObject()

                    // countAsFullWord
                    .startObject("countAsFullWord").field("type", "long").field("index", "no").endObject()

                    // countAsEdgeGram
                    .startObject("countAsEdgeGram").field("type", "long").field("index", "no").endObject()

                    // field stats start
                    // >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
                    .startObject("fieldStats")

                    .field("type", "nested")

                    .startObject("properties")

                    // name
                    .startObject("name").field("type", "string").field("index", "not_analyzed").endObject()

                    // totalWeight
                    .startObject("totalWeight").field("type", "double").field("index", "no").endObject()

                    // totalCount
                    .startObject("totalCount").field("type", "long").field("index", "no").endObject()

                    // countAsFullWord
                    .startObject("countAsFullWord").field("type", "long").field("index", "no").endObject()

                    // countAsEdgeGram
                    .startObject("countAsEdgeGram").field("type", "long").field("index", "no").endObject()

                    .endObject().endObject()
                    // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<


                    // type stats start
                    // >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
                    .startObject("typeStats")

                    .field("type", "nested")

                    .startObject("properties")

                    // name
                    .startObject("name").field("type", "string").field("index", "not_analyzed").endObject()

                    // totalWeight
                    .startObject("totalWeight").field("type", "double").field("index", "no").endObject()

                    // totalCount
                    .startObject("totalCount").field("type", "long").field("index", "no").endObject()

                    // countAsFullWord
                    .startObject("countAsFullWord").field("type", "long").field("index", "no").endObject()

                    // countAsEdgeGram
                    .startObject("countAsEdgeGram").field("type", "long").field("index", "no").endObject()

                    .endObject().endObject()
                    // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

                    // original word stats start
                    // >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
                    .startObject("originalWords")

                    .field("type", "nested")

                    .startObject("properties")

                    // name
                    .startObject("word").field("type", "string").field("index", "no").endObject()

                    // display
                    .startObject("display").field("type", "string").field("index", "no").endObject()

                    // totalWeight
                    .startObject("totalWeight").field("type", "double").field("index", "no").endObject()

                    // totalCount
                    .startObject("totalCount").field("type", "long").field("index", "no").endObject()

                    .endObject().endObject()
                    // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

                    .endObject().endObject().endObject()

                    ;
        } catch (final IOException e) {
            logger.error("IOException in building mapping", e);
            return null;
        }
    }

    private class WatchIndexOpListener extends IndexingOperationListener implements IndexSettingsService.Listener {

        private final AnalysisService analysisService;
        private final Analyzer humaneAnalyzer;
        private final String indexName;

        public WatchIndexOpListener(String indexName, final AnalysisService analysisService) {
            super();
            this.analysisService = analysisService;
            this.indexName = indexName;
            this.humaneAnalyzer = this.analysisService.analyzer("humane_query_analyzer");
        }

        private void add(ParsedDocument parsedDocument) {
            EdgeGramEncodingUtils edgeGramEncodingUtils = new EdgeGramEncodingUtils(1);
            TokenStream tokenStream = null;
            for (ParseContext.Document document : parsedDocument.docs()) {
                for (IndexableField field : document.getFields()) {
//                    logger.info("Parsing field: {}, type: {}", field.name(), field.fieldType().getClass());
                    IndexableFieldType fieldType = field.fieldType();

                    double suggestionWeight;

                    if (fieldType instanceof HumaneTextFieldMapper.HumaneTextFieldType) {
                        suggestionWeight = ((HumaneTextFieldMapper.HumaneTextFieldType) fieldType).getSuggestionWeight();
                    } else if (fieldType instanceof HumaneDescriptiveTextFieldMapper.HumaneDescriptiveTextFieldType) {
                        suggestionWeight = ((HumaneDescriptiveTextFieldMapper.HumaneDescriptiveTextFieldType) fieldType).getSuggestionWeight();
                    } else {
                        continue;
                    }

//                    logger.info("Considering field: {}, weight: {}, class: {}", field.name(), suggestionWeight, fieldType.getClass());

                    tokenStream = field.tokenStream(this.humaneAnalyzer, tokenStream);

                    String fieldName = field.name().substring(0, field.name().length() - 7);

                    try {
                        tokenStream.reset();
                        CharTermAttribute termAttribute = tokenStream.getAttribute(CharTermAttribute.class);

                        String previousWord = null;

                        while (tokenStream.incrementToken()) {
                            String word = termAttribute.toString();

//                            logger.info("Adding word {} for type: {}, index: {}", word, parsedDocument.type(), this.indexName);

                            wordQueue.offer(new WordInfo(this.indexName, parsedDocument.type(), word, fieldName, suggestionWeight, false, word));
                            queuedWordCount.addAndGet(1);

                            if (StringUtils.isNotEmpty(previousWord)) {
                                wordQueue.offer(new BigramWordInfo(this.indexName, parsedDocument.type(), previousWord, word, fieldName, suggestionWeight, false, previousWord + word, previousWord + " " + word));
                                queuedWordCount.addAndGet(1);
                            }

                            for (String edgeGram : edgeGramEncodingUtils.buildEncodings(word)) {
                                if (fieldType instanceof HumaneTextFieldMapper.HumaneTextFieldType || edgeGram.length() > 1) {
                                    wordQueue.offer(new WordInfo(this.indexName, parsedDocument.type(), edgeGram, fieldName, suggestionWeight, true, word));
                                    queuedWordCount.addAndGet(1);
                                }

                                if (StringUtils.isNotEmpty(previousWord)) {
                                    wordQueue.offer(new BigramWordInfo(this.indexName, parsedDocument.type(), previousWord, edgeGram, fieldName, suggestionWeight, true, previousWord + word, previousWord + " " + word));
                                    queuedWordCount.addAndGet(1);
                                }
                            }

                            previousWord = word;
                        }

                        tokenStream.close();
                    } catch (IOException e) {
                        logger.error("IOException in parsing document {}, name {}", e, parsedDocument, field);
                    }
                }
            }

            if (queuedWordCount.get() > 0 && pollerParked.compareAndSet(true, false)) {
                // logger.info("un-parking poller as new words have been queued");
                LockSupport.unpark(poller);
            }
        }

        @Override
        public void postIndex(final Engine.Index index) {
            // logger.info(" {} postIndex -- uid: {}, parsedDoc: {}, version: {}", indexName, index.uid(), index.parsedDoc(), index.version());
            add(index.parsedDoc());
        }

        @Override
        public void postDelete(final Engine.Delete delete) {
            // logger.info(" {} postDelete -- uid: {}, version: {}", indexName, delete.uid(), delete.version());
            // do nothing for now
        }

        @Override
        public void postCreate(final Engine.Create create) {
            // logger.info(" {} postCreate -- uid: {}, parsedDoc: {}, version: {}", indexName, create.uid(), create.parsedDoc(), create.version());
            add(create.parsedDoc());
        }

        @Override
        public void onRefreshSettings(final Settings settings) {
            // recordIndexSettingsChange(settings, indexShard);
            // do nothing;
        }
    }

    static class WordInfo {
        String indexName;
        String typeName;

        String word;
        String field;
        boolean edgeGram;
        String originalWord;
        String originalDisplay;

        double suggestionWeight = 1.0f;

        public WordInfo(String indexName, String typeName, String word, String field, double suggestionWeight, boolean edgeGram, String originalWord) {
            this(indexName, typeName, word, field, suggestionWeight, edgeGram, originalWord, null);
        }

        public WordInfo(String indexName, String typeName, String word, String field, double suggestionWeight, boolean edgeGram, String originalWord, String originalDisplay) {
            this.indexName = indexName;
            this.typeName = typeName;
            this.word = word;
            this.field = field;
            this.suggestionWeight = suggestionWeight;
            this.edgeGram = edgeGram;
            this.originalWord = originalWord;
            this.originalDisplay = originalDisplay;
        }

        @Override
        public String toString() {
            return "{" +
                    "indexName='" + indexName + '\'' +
                    ", typeName='" + typeName + '\'' +
                    ", word='" + word + '\'' +
                    ", field='" + field + '\'' +
                    ", edgeGram=" + edgeGram +
                    ", originalWord='" + originalWord + '\'' +
                    ", originalDisplay='" + originalDisplay + '\'' +
                    ", suggestionWeight=" + suggestionWeight +
                    '}';
        }
    }

    static class BigramWordInfo extends WordInfo {
        String word1;
        String word2;

        public BigramWordInfo(String indexName, String typeName, String word1, String word2, String field, double suggestionWeight, boolean edgeGram, String originalWord, String originalDisplay) {
            super(indexName, typeName, word1 + word2, field, suggestionWeight, edgeGram, originalWord, originalDisplay);

            this.word1 = word1;
            this.word2 = word2;
        }

        @Override
        public String toString() {
            return "{" +
                    "word1='" + word1 + '\'' +
                    ", word2='" + word2 + '\'' +
                    "} " + super.toString();
        }
    }

    static class WordAggregateInfo {
        final String indexName;

        final String word;

        int totalCount;
        int countAsFullWord;
        int countAsEdgeGram;
        double totalWeight;

        final Set<String> encodings = new HashSet<>();

        final Map<String, SubAggregateInfo> typeAggregateInfo = new HashMap<>();

        final Map<String, SubAggregateInfo> fieldAggregateInfo = new HashMap<>();

        final Map<String, OriginalWordInfo> originalWords = new HashMap<>();

        public WordAggregateInfo(String indexName, String word) {
            this.indexName = indexName;
            this.word = word;
        }

        public Map<String, Object> map() {
            Map<String, Object> map = new HashMap<>();

            map.put("word", word);
            map.put("totalCount", totalCount);
            map.put("totalWeight", totalWeight);
            map.put("countAsFullWord", countAsFullWord);
            map.put("countAsEdgeGram", countAsEdgeGram);
            map.put("encodings", encodings);
            map.put("typeStats", typeAggregateInfo.values().stream().map(SubAggregateInfo::map).collect(Collectors.toList()));
            map.put("fieldStats", fieldAggregateInfo.values().stream().map(SubAggregateInfo::map).collect(Collectors.toList()));

            List<Map<String, Object>> originalWordList = new LinkedList<>();

            for (OriginalWordInfo originalWordInfo : originalWords.values()) {
                originalWordList.add(originalWordInfo.map());
            }

            map.put("originalWords", originalWordList);

            return map;
        }

        @SuppressWarnings("unchecked")
        protected static WordAggregateInfo unmap(WordAggregateInfo wordAggregateInfo, Map<String, Object> map) {
            wordAggregateInfo.totalCount = (int) map.get("totalCount");
            wordAggregateInfo.totalWeight = (double) map.get("totalWeight");
            wordAggregateInfo.countAsFullWord = (int) map.get("countAsFullWord");
            wordAggregateInfo.countAsEdgeGram = (int) map.get("countAsEdgeGram");

            List<Map<String, Object>> typeStats = (List<Map<String, Object>>) map.get("typeStats");
            for (Map<String, Object> stat : typeStats) {
                wordAggregateInfo.typeAggregateInfo.put((String) stat.get("name"), SubAggregateInfo.unmap(stat));
            }

            List<Map<String, Object>> fieldStats = (List<Map<String, Object>>) map.get("fieldStats");
            for (Map<String, Object> stat : fieldStats) {
                wordAggregateInfo.fieldAggregateInfo.put((String) stat.get("name"), SubAggregateInfo.unmap(stat));
            }

            List<Map<String, Object>> originalWordList = (List<Map<String, Object>>) map.get("originalWords");
            for (Map<String, Object> originalWordMap : originalWordList) {
                OriginalWordInfo originalWordInfo = OriginalWordInfo.unmap(originalWordMap);
                wordAggregateInfo.originalWords.put(originalWordInfo.word, originalWordInfo);
            }

            wordAggregateInfo.encodings.addAll((List<String>) map.get("encodings"));

            return wordAggregateInfo;
        }

        @SuppressWarnings("unchecked")
        public static WordAggregateInfo unmap(String indexName, Map<String, Object> map) {
            return unmap(new WordAggregateInfo(indexName, (String) map.get("word")), map);
        }

        @Override
        public String toString() {
            return "{" +
                    "indexName='" + indexName + '\'' +
                    ", word='" + word + '\'' +
                    ", totalCount=" + totalCount +
                    ", countAsFullWord=" + countAsFullWord +
                    ", countAsEdgeGram=" + countAsEdgeGram +
                    ", totalWeight=" + totalWeight +
                    ", encodings=" + encodings +
                    ", typeAggregateInfo=" + typeAggregateInfo +
                    ", fieldAggregateInfo=" + fieldAggregateInfo +
                    ", originalWords=" + originalWords +
                    '}';
        }
    }

    static class BigramWordAggregateInfo extends WordAggregateInfo {
        final String word1;
        final String word2;

        final Set<String> word1Encodings = new HashSet<>();
        final Set<String> word2Encodings = new HashSet<>();

        public BigramWordAggregateInfo(String indexName, String word1, String word2) {
            super(indexName, word1 + word2);
            this.word1 = word1;
            this.word2 = word2;
        }

        public Map<String, Object> map() {
            Map<String, Object> map = super.map();
            map.put("word1", word1);
            map.put("word2", word2);
            map.put("word1Encodings", word1Encodings);
            map.put("word2Encodings", word2Encodings);

            return map;
        }

        @SuppressWarnings("unchecked")
        public static BigramWordAggregateInfo unmap(String indexName, Map<String, Object> map) {
            BigramWordAggregateInfo wordAggregateInfo = new BigramWordAggregateInfo(indexName, (String) map.get("word1"), (String) map.get("word2"));

            WordAggregateInfo.unmap(wordAggregateInfo, map);

            wordAggregateInfo.word1Encodings.addAll((List<String>) map.get("word1Encodings"));
            wordAggregateInfo.word2Encodings.addAll((List<String>) map.get("word2Encodings"));

            return wordAggregateInfo;
        }

        @Override
        public String toString() {
            return "{" +
                    "word1='" + word1 + '\'' +
                    ", word2='" + word2 + '\'' +
                    ", word1Encodings=" + word1Encodings +
                    ", word2Encodings=" + word2Encodings +
                    "} " + super.toString();
        }
    }

    static class SubAggregateInfo {
        String name;
        double totalWeight;
        int totalCount;
        int countAsFullWord;
        int countAsEdgeGram;

        public SubAggregateInfo(String name, double totalWeight, int totalCount, int countAsFullWord, int countAsEdgeGram) {
            this.name = name;
            this.totalWeight = totalWeight;
            this.totalCount = totalCount;
            this.countAsFullWord = countAsFullWord;
            this.countAsEdgeGram = countAsEdgeGram;
        }

        public Map<String, Object> map() {
            Map<String, Object> map = new HashMap<>();

            map.put("name", name);
            map.put("totalWeight", totalWeight);
            map.put("totalCount", totalCount);
            map.put("countAsFullWord", countAsFullWord);
            map.put("countAsEdgeGram", countAsEdgeGram);

            return map;
        }

        public static SubAggregateInfo unmap(Map<String, Object> map) {
            return new SubAggregateInfo((String) map.get("name"), (double) map.get("totalWeight"), (int) map.get("totalCount"), (int) map.get("countAsFullWord"), (int) map.get("countAsEdgeGram"));
        }

        @Override
        public String toString() {
            return "{" +
                    "name='" + name + '\'' +
                    ", totalWeight=" + totalWeight +
                    ", totalCount=" + totalCount +
                    ", countAsFullWord=" + countAsFullWord +
                    ", countAsEdgeGram=" + countAsEdgeGram +
                    '}';
        }
    }

    static class OriginalWordInfo {
        String word;
        String display;
        double totalWeight;
        int totalCount;

        public OriginalWordInfo(String word, String display, double totalWeight, int totalCount) {
            this.word = word;
            this.display = display;
            this.totalWeight = totalWeight;
            this.totalCount = totalCount;
        }

        public Map<String, Object> map() {
            Map<String, Object> map = new HashMap<>();

            map.put("word", word);
            map.put("display", display);
            map.put("totalWeight", totalWeight);
            map.put("totalCount", totalCount);

            return map;
        }

        public static OriginalWordInfo unmap(Map<String, Object> map) {
            return new OriginalWordInfo((String) map.get("word"), (String) map.get("display"), (double) map.get("totalWeight"), (int) map.get("totalCount"));
        }

        @Override
        public String toString() {
            return "{" +
                    "word='" + word + '\'' +
                    ", display='" + display + '\'' +
                    ", totalWeight=" + totalWeight +
                    ", totalCount=" + totalCount +
                    '}';
        }
    }
}

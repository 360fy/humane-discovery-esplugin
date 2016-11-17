package io.threesixtyfy.humaneDiscovery.service.wordIndex;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;

public class IndexManager extends Thread {

    private static final String NAME = "index-manager";
    private static final String TYPE_FIELD = "type";
    private static final String STRING_VALUE = "string";
    private static final String INDEX_FIELD = "index";
    private static final String NOT_ANALYZED_VALUE = "not_analyzed";
    private static final String STORE_FIELD = "store";
    private static final String NO_VALUE = "no";
    private static final String DOUBLE_VALUE = "double";
    private static final String LONG_VALUE = "long";
    private static final String INCLUDE_IN_ALL_FIELD = "include_in_all";
    private static final String DYNAMIC_FIELD = "dynamic";
    private static final String FALSE_VALUE = "false";
    private static final String PROPERTIES_FIELD = "properties";
    private static final String NESTED_VALUE = "nested";

    private static final Logger logger = Loggers.getLogger(IndexManager.class);

    private final SharedChannel sharedChannel;
    private final Client client;
    private final Set<String> wordIndices = new HashSet<>();

    public IndexManager(SharedChannel sharedChannel, Client client) {
        this.setDaemon(true);
        this.setName(NAME);
        this.sharedChannel = sharedChannel;
        this.client = client;
    }

    @Override
    public void run() {
        this.runIndexerManager();
    }

    public void shutdown() {
        // stop the indexManager thread too
        sharedChannel.getIndexOperationsQueue().add(new WordIndexCrudRequest(WordIndexCrudRequest.RequestType.STOP));
    }

    private void runIndexerManager() {
        // handle requests until we are interrupted
        while (!Thread.interrupted()) {
            try {
                // block until a request arrives
                WordIndexCrudRequest request = sharedChannel.getIndexOperationsQueue().take();
                if (request.getRequestType() == WordIndexCrudRequest.RequestType.STOP) {
                    break;
                }

                // do the operation
                if (request.getRequestType() == WordIndexCrudRequest.RequestType.CREATE) {
                    createWordIndex(request.getIndexName());
                } else if (request.getRequestType() == WordIndexCrudRequest.RequestType.DELETE) {
                    deleteWordIndex(request.getIndexName());
                }

            } catch (InterruptedException ie) {
                // stop
                break;
            }
        }
    }

    private void createWordIndex(final String indexName) {
        final String wordIndexName = wordIndexName(indexName);

        if (wordIndices.contains(wordIndexName)) {
            return;
        }

        wordIndices.add(wordIndexName);

        client.admin().indices().prepareExists(wordIndexName).execute(new ActionListener<IndicesExistsResponse>() {
            @Override
            public void onResponse(final IndicesExistsResponse response) {
                if (response.isExists()) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("{} index exists already.", wordIndexName);
                    }
                } else {
                    logger.info("will create {} index", wordIndexName);

                    final Settings indexSettings = Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0).build();

                    client.admin()
                            .indices()
                            .prepareCreate(wordIndexName)
                            .addMapping(WordIndexConstants.UNIGRAM_WORD_INDEX_TYPE, getUnigramMapping())
                            .addMapping(WordIndexConstants.BIGRAM_WORD_INDEX_TYPE, getBigramMapping())
                            .setSettings(indexSettings)
                            .execute(new ActionListener<CreateIndexResponse>() {
                                @Override
                                public void onResponse(final CreateIndexResponse response) {
                                    if (!response.isAcknowledged()) {
                                        logger.error("failed to create {}.", wordIndexName);
                                        throw new ElasticsearchException("Failed to create index " + wordIndexName);
                                    } else {
                                        logger.info("successfully created {}.", wordIndexName);
                                    }
                                }

                                @Override
                                public void onFailure(final Exception e) {
                                    logger.error("failed to create {}", e, wordIndexName);
                                }
                            });
                }
            }

            @Override
            public void onFailure(final Exception e) {
                logger.error("The state of {} index is invalid.", e, wordIndexName);
            }
        });
    }

    private void deleteWordIndex(final String indexName) {
        final String wordIndexName = wordIndexName(indexName);

        if (!wordIndices.contains(wordIndexName)) {
            return;
        }

        wordIndices.remove(wordIndexName);

        client.admin().indices().prepareExists(wordIndexName).execute(new ActionListener<IndicesExistsResponse>() {
            @Override
            public void onResponse(final IndicesExistsResponse response) {
                if (response.isExists()) {
                    logger.info("will delete {} index", wordIndexName);

                    client.admin().indices().prepareDelete(wordIndexName).execute(new ActionListener<DeleteIndexResponse>() {
                        @Override
                        public void onResponse(final DeleteIndexResponse response) {
                            if (!response.isAcknowledged()) {
                                logger.error("failed to delete {}.", wordIndexName);
                                throw new ElasticsearchException("Failed to create index " + wordIndexName);
                            } else {
                                logger.info("successfully deleted {}.", wordIndexName);
                            }
                        }

                        @Override
                        public void onFailure(final Exception e) {
                            logger.error("failed to delete {}", e, wordIndexName);
                        }
                    });
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug("{} index does not exist.", wordIndexName);
                    }
                }
            }

            @Override
            public void onFailure(final Exception e) {
                logger.error("The state of {} index is invalid.", e, wordIndexName);
            }
        });
    }

    private String wordIndexName(String indexName) {
        return indexName + WordIndexConstants.WORD_INDEX_STORE_SUFFIX;
    }

    private XContentBuilder getUnigramMapping() {
        try {
            return XContentBuilder.builder(JsonXContent.jsonXContent).startObject().startObject(WordIndexConstants.UNIGRAM_WORD_INDEX_TYPE)
                    .field(INCLUDE_IN_ALL_FIELD, FALSE_VALUE)
                    .field(DYNAMIC_FIELD, FALSE_VALUE)

                    .startObject(PROPERTIES_FIELD)

                    // word
                    .startObject(WordIndexConstants.WORD_FIELD).field(TYPE_FIELD, STRING_VALUE).field(INDEX_FIELD, NOT_ANALYZED_VALUE).field(STORE_FIELD, NO_VALUE).endObject()

                    // encodings
                    .startObject(WordIndexConstants.ENCODINGS_FIELD).field(TYPE_FIELD, STRING_VALUE).field(INDEX_FIELD, NOT_ANALYZED_VALUE).field(STORE_FIELD, NO_VALUE).endObject()

                    // weight
                    .startObject(WordIndexConstants.TOTAL_WEIGHT_FIELD).field(TYPE_FIELD, DOUBLE_VALUE).field(STORE_FIELD, NO_VALUE).endObject()

                    // totalCount
                    .startObject(WordIndexConstants.TOTAL_COUNT_FIELD).field(TYPE_FIELD, LONG_VALUE).field(STORE_FIELD, NO_VALUE).endObject()

                    // countAsFullWord
                    .startObject(WordIndexConstants.COUNT_AS_FULL_WORD_FIELD).field(TYPE_FIELD, LONG_VALUE).field(STORE_FIELD, NO_VALUE).endObject()

                    // countAsEdgeGram
                    .startObject(WordIndexConstants.COUNT_AS_EDGE_GRAM_FIELD).field(TYPE_FIELD, LONG_VALUE).field(STORE_FIELD, NO_VALUE).endObject()

                    // fieldName stats start
                    // >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
                    .startObject(WordIndexConstants.FIELD_STATS_FIELD)

                    .field(TYPE_FIELD, NESTED_VALUE)

                    .startObject(PROPERTIES_FIELD)

                    // fieldName
                    .startObject(WordIndexConstants.FIELD_NAME_FIELD).field(TYPE_FIELD, STRING_VALUE).field(INDEX_FIELD, NOT_ANALYZED_VALUE).field(STORE_FIELD, NO_VALUE).endObject()

                    // totalWeight
                    .startObject(WordIndexConstants.TOTAL_WEIGHT_FIELD).field(TYPE_FIELD, DOUBLE_VALUE).field(STORE_FIELD, NO_VALUE).endObject()

                    // totalCount
                    .startObject(WordIndexConstants.TOTAL_COUNT_FIELD).field(TYPE_FIELD, LONG_VALUE).field(STORE_FIELD, NO_VALUE).endObject()

                    // countAsFullWord
                    .startObject(WordIndexConstants.COUNT_AS_FULL_WORD_FIELD).field(TYPE_FIELD, LONG_VALUE).field(STORE_FIELD, NO_VALUE).endObject()

                    // countAsEdgeGram
                    .startObject(WordIndexConstants.COUNT_AS_EDGE_GRAM_FIELD).field(TYPE_FIELD, LONG_VALUE).field(STORE_FIELD, NO_VALUE).endObject()

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
            return XContentBuilder.builder(JsonXContent.jsonXContent).startObject().startObject(WordIndexConstants.BIGRAM_WORD_INDEX_TYPE)
                    .field(INCLUDE_IN_ALL_FIELD, FALSE_VALUE)
                    .field(DYNAMIC_FIELD, FALSE_VALUE)

                    .startObject(PROPERTIES_FIELD)

                    // word
                    .startObject(WordIndexConstants.WORD_FIELD).field(TYPE_FIELD, STRING_VALUE).field(INDEX_FIELD, NOT_ANALYZED_VALUE).field(STORE_FIELD, NO_VALUE).endObject()

                    // word1
                    .startObject(WordIndexConstants.WORD_1_FIELD).field(TYPE_FIELD, STRING_VALUE).field(INDEX_FIELD, NOT_ANALYZED_VALUE).field(STORE_FIELD, NO_VALUE).endObject()

                    // word2
                    .startObject(WordIndexConstants.WORD_2_FIELD).field(TYPE_FIELD, STRING_VALUE).field(INDEX_FIELD, NOT_ANALYZED_VALUE).field(STORE_FIELD, NO_VALUE).endObject()

                    // encodings
                    .startObject(WordIndexConstants.ENCODINGS_FIELD).field(TYPE_FIELD, STRING_VALUE).field(INDEX_FIELD, NOT_ANALYZED_VALUE).field(STORE_FIELD, NO_VALUE).endObject()

                    // word1 encodings
                    .startObject(WordIndexConstants.WORD_1_ENCODINGS_FIELD).field(TYPE_FIELD, STRING_VALUE).field(INDEX_FIELD, NOT_ANALYZED_VALUE).field(STORE_FIELD, NO_VALUE).endObject()

                    // word2 encodings
                    .startObject(WordIndexConstants.WORD_2_ENCODINGS_FIELD).field(TYPE_FIELD, STRING_VALUE).field(INDEX_FIELD, NOT_ANALYZED_VALUE).field(STORE_FIELD, NO_VALUE).endObject()

                    // weight
                    .startObject(WordIndexConstants.TOTAL_WEIGHT_FIELD).field(TYPE_FIELD, DOUBLE_VALUE).field(STORE_FIELD, NO_VALUE).endObject()

                    // totalCount
                    .startObject(WordIndexConstants.TOTAL_COUNT_FIELD).field(TYPE_FIELD, LONG_VALUE).field(STORE_FIELD, NO_VALUE).endObject()

                    // countAsFullWord
                    .startObject(WordIndexConstants.COUNT_AS_FULL_WORD_FIELD).field(TYPE_FIELD, LONG_VALUE).field(STORE_FIELD, NO_VALUE).endObject()

                    // countAsEdgeGram
                    .startObject(WordIndexConstants.COUNT_AS_EDGE_GRAM_FIELD).field(TYPE_FIELD, LONG_VALUE).field(STORE_FIELD, NO_VALUE).endObject()

                    // fieldName stats start
                    // >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
                    .startObject(WordIndexConstants.FIELD_STATS_FIELD)

                    .field(TYPE_FIELD, NESTED_VALUE)

                    .startObject(PROPERTIES_FIELD)

                    // fieldName
                    .startObject(WordIndexConstants.FIELD_NAME_FIELD).field(TYPE_FIELD, STRING_VALUE).field(INDEX_FIELD, NOT_ANALYZED_VALUE).field(STORE_FIELD, NO_VALUE).endObject()

                    // totalWeight
                    .startObject(WordIndexConstants.TOTAL_WEIGHT_FIELD).field(TYPE_FIELD, DOUBLE_VALUE).field(STORE_FIELD, NO_VALUE).endObject()

                    // totalCount
                    .startObject(WordIndexConstants.TOTAL_COUNT_FIELD).field(TYPE_FIELD, LONG_VALUE).field(STORE_FIELD, NO_VALUE).endObject()

                    // countAsFullWord
                    .startObject(WordIndexConstants.COUNT_AS_FULL_WORD_FIELD).field(TYPE_FIELD, LONG_VALUE).field(STORE_FIELD, NO_VALUE).endObject()

                    // countAsEdgeGram
                    .startObject(WordIndexConstants.COUNT_AS_EDGE_GRAM_FIELD).field(TYPE_FIELD, LONG_VALUE).field(STORE_FIELD, NO_VALUE).endObject()

                    .endObject().endObject()
                    // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

                    .endObject().endObject().endObject()

                    ;
        } catch (final IOException e) {
            logger.error("IOException in building mapping", e);
            return null;
        }
    }
}

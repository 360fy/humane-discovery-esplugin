package io.threesixtyfy.humaneDiscovery.core.tokenIndex;

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

import static io.threesixtyfy.humaneDiscovery.core.tokenIndex.TokenIndexConstants.tokenIndexName;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;

public class IndexManager extends Thread {

    private static final String NAME = "index-manager";
    private static final String TYPE_FIELD = "type";
    private static final String OBJECT_TYPE = "object";

//    private static final String BOOLEAN_TYPE = "boolean";
//    private static final String INTEGER_TYPE = "integer";

    private static final String KEYWORD_TYPE = "keyword";
    private static final String DOUBLE_TYPE = "double";
    private static final String LONG_TYPE = "long";
    private static final String INCLUDE_IN_ALL_FIELD = "include_in_all";
    private static final String DYNAMIC_FIELD = "dynamic";
    private static final String FALSE_VALUE = "false";
    private static final String PROPERTIES_FIELD = "properties";
    private static final String NESTED_TYPE = "nested";

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
        sharedChannel.getIndexOperationsQueue().add(new TokenIndexCrudRequest(TokenIndexCrudRequest.RequestType.STOP));
    }

    private void runIndexerManager() {
        // handle requests until we are interrupted
        while (!Thread.interrupted()) {
            try {
                // block until a request arrives
                TokenIndexCrudRequest request = sharedChannel.getIndexOperationsQueue().take();
                if (request.getRequestType() == TokenIndexCrudRequest.RequestType.STOP) {
                    break;
                }

                // do the operation
                if (request.getRequestType() == TokenIndexCrudRequest.RequestType.CREATE) {
                    createTokenIndex(request.getIndexName());
                } else if (request.getRequestType() == TokenIndexCrudRequest.RequestType.DELETE) {
                    deleteWordIndex(request.getIndexName());
                }

            } catch (InterruptedException ie) {
                // stop
                break;
            }
        }
    }

    private void createTokenIndex(final String indexName) {
        final String wordIndexName = tokenIndexName(indexName);

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
                            .addMapping(TokenIndexConstants.TOKEN_INDEX_TYPE, getMapping())
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
        final String wordIndexName = tokenIndexName(indexName);

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

    private XContentBuilder getMapping() {
        try {
            return XContentBuilder.builder(JsonXContent.jsonXContent).startObject().startObject(TokenIndexConstants.TOKEN_INDEX_TYPE)
                    .field(INCLUDE_IN_ALL_FIELD, FALSE_VALUE)
                    .field(DYNAMIC_FIELD, FALSE_VALUE)

                    .startObject(PROPERTIES_FIELD)

                    // key
                    .startObject(TokenIndexConstants.Fields.KEY).field(TYPE_FIELD, KEYWORD_TYPE).endObject()

                    // tokens
                    .startObject(TokenIndexConstants.Fields.TOKENS).field(TYPE_FIELD, KEYWORD_TYPE).endObject()

                    // totalCount
                    .startObject(TokenIndexConstants.Fields.TOTAL_COUNT).field(TYPE_FIELD, LONG_TYPE).endObject()

                    // totalCount
                    .startObject(TokenIndexConstants.Fields.TOKEN_COUNT).field(TYPE_FIELD, LONG_TYPE).endObject()

                    // totalCount
                    .startObject(TokenIndexConstants.Fields.SCOPES).field(TYPE_FIELD, KEYWORD_TYPE).endObject()

                    // encodings start
                    // >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
                    .startObject(TokenIndexConstants.Fields.ENCODINGS)

                    .field(TYPE_FIELD, NESTED_TYPE)

                    .startObject(PROPERTIES_FIELD)

                    // position
//                    .startObject(TokenIndexConstants.Fields.POSITION).field(TYPE_FIELD, INTEGER_TYPE).endObject()

                    // token
                    .startObject(TokenIndexConstants.Fields.TOKEN).field(TYPE_FIELD, KEYWORD_TYPE).endObject()

                    // encodings
                    .startObject(TokenIndexConstants.Fields.ENCODINGS)
                    .field(TYPE_FIELD, OBJECT_TYPE)
                    .startObject(PROPERTIES_FIELD)
                    .startObject(TokenIndexConstants.Encoding.RS_ENCODING).field(TYPE_FIELD, KEYWORD_TYPE).endObject()
                    .startObject(TokenIndexConstants.Encoding.DS_ENCODING).field(TYPE_FIELD, KEYWORD_TYPE).endObject()
                    .startObject(TokenIndexConstants.Encoding.BM_ENCODING).field(TYPE_FIELD, KEYWORD_TYPE).endObject()
                    .startObject(TokenIndexConstants.Encoding.DM_ENCODING).field(TYPE_FIELD, KEYWORD_TYPE).endObject()
                    .startObject(TokenIndexConstants.Encoding.NGRAM_ENCODING).field(TYPE_FIELD, KEYWORD_TYPE).endObject()
                    .startObject(TokenIndexConstants.Encoding.NGRAM_START_ENCODING).field(TYPE_FIELD, KEYWORD_TYPE).endObject()
                    .startObject(TokenIndexConstants.Encoding.NGRAM_END_ENCODING).field(TYPE_FIELD, KEYWORD_TYPE).endObject()
                    .endObject()
                    .endObject()

                    // tokenType
                    .startObject(TokenIndexConstants.Fields.TOKEN_TYPE).field(TYPE_FIELD, KEYWORD_TYPE).endObject()

                    // weight
                    .startObject(TokenIndexConstants.Fields.WEIGHT).field(TYPE_FIELD, DOUBLE_TYPE).endObject()

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

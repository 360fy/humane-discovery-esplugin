package io.threesixtyfy.humaneDiscovery.api.autocomplete;

import io.threesixtyfy.humaneDiscovery.api.commons.QueryResponse;
import io.threesixtyfy.humaneDiscovery.api.commons.TransportQueryAction;
import io.threesixtyfy.humaneDiscovery.core.cache.CacheService;
import io.threesixtyfy.humaneDiscovery.core.instance.InstanceContext;
import io.threesixtyfy.humaneDiscovery.core.tag.BaseTag;
import io.threesixtyfy.humaneDiscovery.core.tag.IntentTag;
import io.threesixtyfy.humaneDiscovery.core.tag.NGramTag;
import io.threesixtyfy.humaneDiscovery.core.tag.TagType;
import io.threesixtyfy.humaneDiscovery.core.tagForest.ForestMember;
import io.threesixtyfy.humaneDiscovery.core.tagForest.TagForest;
import io.threesixtyfy.humaneDiscovery.core.tagForest.TagGraph;
import io.threesixtyfy.humaneDiscovery.core.tagForest.TagNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.DisMaxQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.ScriptSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;


public class TransportAutocompleteAction extends TransportQueryAction<AutocompleteQuerySource, AutocompleteQueryRequest, QueryResponse> {

    private static final Logger logger = Loggers.getLogger(TransportAutocompleteAction.class);

    private static final String NEW_CAR_TYPE = "new_car";
    private static final String USED_CAR_TYPE = "used_car";
    private static final String CAR_NEWS_TYPE = "car_news";
    private static final String NEW_CAR_DEALER_TYPE = "new_car_dealer";


    private static final String NEW_CAR_SECTION = "new_car";

    private static final String NEW_CAR_TITLE = "New Cars";
    private static final String USED_CAR_TITLE = "Used Cars";
    private static final String NEW_CAR_DEALER_TITLE = "New Car Dealers";
    private static final String CAR_NEWS_TITLE = "News";

    private static final String NEW_CAR_BRAND_TYPE = "new_car_brand";
    private static final String NEW_CAR_MODEL_TYPE = "new_car_model";
    private static final String NEW_CAR_VARIANT_TYPE = "new_car_variant";

    private static final String[] NEW_CAR_BRAND_AND_UP = {NEW_CAR_BRAND_TYPE, NEW_CAR_MODEL_TYPE, NEW_CAR_VARIANT_TYPE};
    private static final String[] NEW_CAR_MODEL_AND_UP = {NEW_CAR_MODEL_TYPE, NEW_CAR_VARIANT_TYPE};
    private static final String[] NEW_CAR_VARIANT_AND_UP = {NEW_CAR_VARIANT_TYPE};

    private static final String BRAND_FIELD = "brand";
    private static final String BRAND_INTENT = BRAND_FIELD;
    private static final String MODEL_FIELD = "model";
    private static final String MODEL_INTENT = MODEL_FIELD;
    private static final String VARIANT_FIELD = "variant";
    private static final String VARIANT_INTENT = VARIANT_FIELD;

    private static final String MODEL_STATUS_FIELD = "modelStatus";

    private static final String SORT_BY_NEW_CAR_TYPE_SCRIPT = "doc['_type'].value == 'new_car_model_page' || doc['_type'].value == 'new_car_variant_page' ? 0 : (doc['_type'].value == 'new_car_brand' ? 1 : (doc['_type'].value == 'new_car_model' ? 2 : 3))";
    private static final String[] INCLUDED_FIELDS = {
            "brand",
            "brandKey",
            "brandUrl",
            "model",
            "modelKey",
            "modelUrl",
            "variant",
            "variantKey",
            "variantUrl",
            "_type",
            "_score",
            "pages"
    };

    private static Map<String, String> SectionTitles = new HashMap<>();
    private static Map<String, String> NewCarFields = new HashMap<>();

    static {
        SectionTitles.put(NEW_CAR_TYPE, NEW_CAR_TITLE);
        SectionTitles.put(USED_CAR_TYPE, USED_CAR_TITLE);
        SectionTitles.put(CAR_NEWS_TYPE, CAR_NEWS_TITLE);
        SectionTitles.put(NEW_CAR_DEALER_TYPE, NEW_CAR_DEALER_TITLE);
    }

    static {
        NewCarFields.put(NEW_CAR_BRAND_TYPE, BRAND_FIELD);
        NewCarFields.put(NEW_CAR_MODEL_TYPE, MODEL_FIELD);
        NewCarFields.put(NEW_CAR_VARIANT_TYPE, VARIANT_FIELD);
    }

    private final IndicesOptions indicesOptions = IndicesOptions.strictExpandOpenAndForbidClosed();

    @Inject
    public TransportAutocompleteAction(Settings settings,
                                       ThreadPool threadPool,
                                       TransportService transportService,
                                       ActionFilters actionFilters,
                                       IndexNameExpressionResolver indexNameExpressionResolver,
                                       ClusterService clusterService,
                                       IndicesService indicesService,
                                       Client client,
                                       CacheService cacheService) throws IOException {
        super(settings, AutocompleteAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, clusterService, indicesService, client, AutocompleteQueryRequest::new, cacheService);
    }

    protected QueryResponse response(AutocompleteQueryRequest queryRequest) throws IOException, IllegalAccessException, InstantiationException, ClassNotFoundException {
        return this.cacheService.getOrCompute(queryRequest.key(), QueryResponse.class, () -> {
            QueryResponse queryResponse = null;
            Index[] indices = indexNameExpressionResolver.concreteIndices(clusterService.state(), indicesOptions, StringUtils.lowerCase(queryRequest.instance()) + "_store");

            InstanceContext instanceContext = this.instanceContexts.get(queryRequest.instance());

            // TODO: support type specific searches
            if (indices != null && indices.length == 1) {
                Index index = indices[0];

                List<TagForest> tagForests = createIntents(queryRequest, instanceContext);
                if (tagForests != null && tagForests.size() > 0) {
                    queryResponse = buildSearch(index.getName(), queryRequest, tagForests.get(0));

                }
            }

            queryResponse = queryResponse == null ? new AutocompleteResponse(queryRequest.querySource().query()) : queryResponse;

            return queryResponse;
        });
    }

    // dealer section will come by car name or dealer name only, can be filtered by city
    // news section will also come by car name only
    private QueryResponse buildSearch(String indexName, AutocompleteQueryRequest autocompleteQueryRequest, TagForest tagForest) {
        // here we do all type of searches
        if (isCarNameSearch(tagForest)) {
            List<ForestMember> carNames = new ArrayList<>();
            CarNameType carNameType = extractCarNames(tagForest, carNames::add);

            return searchNewCarsByCarName(indexName, autocompleteQueryRequest, carNameType, carNames);
        }

        // over car name, optional filters such as car type, fuel type, seating capacity, automatic transmission can be applied
        // when such features are needed => brand shall not be active
        return null;
    }

    private boolean isCarNameSearch(TagForest tagForest) {
        // if there is a forest member with intent tag of brand / model / variant type OR NGRAM tag with ref to Intent of brand / model / variant
        for (ForestMember forestMember : tagForest.getMembers()) {
            for (BaseTag tag : forestMember.getTags()) {
                if (tag instanceof IntentTag) {
                    IntentTag intentTag = (IntentTag) tag;

                    if (StringUtils.equals(intentTag.getName(), BRAND_INTENT) || StringUtils.equals(intentTag.getName(), MODEL_INTENT) || StringUtils.equals(intentTag.getName(), VARIANT_INTENT)) {
                        return true;
                    }
                } else if (tag instanceof NGramTag) {
                    NGramTag nGramTag = (NGramTag) tag;

                    if (nGramTag.getRefTagType() == TagType.Intent
                            && (StringUtils.equals(nGramTag.getName(), BRAND_INTENT) || StringUtils.equals(nGramTag.getName(), MODEL_INTENT) || StringUtils.equals(nGramTag.getName(), VARIANT_INTENT))) {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    private CarNameType extractCarNames(TagForest tagForest, Consumer<ForestMember> consumer) {
        int maxLevel = -1;
        for (ForestMember forestMember : tagForest.getMembers()) {

            boolean hasBrand = false;
            boolean hasModel = false;
            boolean hasVariant = false;
            for (BaseTag tag : forestMember.getTags()) {
                if (tag instanceof IntentTag) {
                    IntentTag intentTag = (IntentTag) tag;

                    if (StringUtils.equals(intentTag.getName(), BRAND_INTENT)) {
                        hasBrand = true;
                    } else if (StringUtils.equals(intentTag.getName(), MODEL_INTENT)) {
                        hasModel = true;
                    } else if (StringUtils.equals(intentTag.getName(), VARIANT_INTENT)) {
                        hasVariant = true;
                    }
                } else if (tag instanceof NGramTag) {
                    NGramTag nGramTag = (NGramTag) tag;

                    if (nGramTag.getRefTagType() == TagType.Intent && StringUtils.equals(nGramTag.getName(), BRAND_INTENT)) {
                        hasBrand = true;
                    } else if (nGramTag.getRefTagType() == TagType.Intent && StringUtils.equals(nGramTag.getName(), MODEL_INTENT)) {
                        hasModel = true;
                    } else if (nGramTag.getRefTagType() == TagType.Intent && StringUtils.equals(nGramTag.getName(), VARIANT_INTENT)) {
                        hasVariant = true;
                    }
                }
            }

            int level = -1;
            if (hasBrand) {
                level = 0;
            } else if (hasModel) {
                level = 1;
            } else if (hasVariant) {
                level = 2;
            }

            maxLevel = Math.max(maxLevel, level);
            if (level != -1) {
                consumer.accept(forestMember);
            }
        }

        if (maxLevel == -1) {
            return null;
        }

        return CarNameType.fromLevel(maxLevel);
    }

    private QueryBuilder fieldQuery(String field, String token) {
        return QueryBuilders.constantScoreQuery(QueryBuilders.termQuery(field, token));
    }

    // TODO: do we need to query anything special for edge grams
    // TODO: do we need to query anything special for shingles
    private QueryBuilder fieldQuery(String field, List<String> tokens) {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        for (String token : tokens) {
            boolQueryBuilder.must(fieldQuery(field, token));
        }

        return boolQueryBuilder;
    }

    private QueryBuilder fieldQuery(String field, ForestMember forestMember) {
        if (field == null) {
            return null;
        }

        if (forestMember instanceof TagGraph) {
            return fieldQuery(field, ((TagGraph) forestMember).getMatchedTokens());
        } else if (forestMember instanceof TagNode) {
            return fieldQuery(field, ((TagNode) forestMember).getMatchedToken());
        }

        return null;
    }

    private QueryBuilder newCarNameQueryByType(String type, ForestMember forestMember) {
        String field = NewCarFields.get(type);

        QueryBuilder queryBuilder = fieldQuery(field, forestMember);
        if (queryBuilder == null) {
            return null;
        }

        return QueryBuilders.boolQuery().must(queryBuilder).filter(QueryBuilders.typeQuery(type));
    }

    private QueryBuilder newCarNameQueryByTypes(String[] types, ForestMember carName) {
        if (types.length == 1) {
            return newCarNameQueryByType(types[0], carName);
        } else {
            DisMaxQueryBuilder disMaxQueryBuilder = QueryBuilders.disMaxQuery();
            for (String type : types) {
                QueryBuilder typeQuery = newCarNameQueryByType(type, carName);
                if (typeQuery != null) {
                    disMaxQueryBuilder.add(typeQuery);
                }
            }

            return disMaxQueryBuilder;
        }
    }

    // TODO: do we relax queries in some ways
    private QueryBuilder newCarsQuery(CarNameType carNameType, List<ForestMember> carNames) {
        // if it has variant part, then we form variant query and optional model / brand in new_car_variant
        // if it has model part, then we form model query and optional brand in new_car_model and new_car_variant
        // if it has brand query, then we form brand query in new_car_brand, new_car_model, new_car_variant
        if (carNames == null || carNames.size() == 0) {
            return null;
        }

        String[] types = newCarsTypes(carNameType);
        if (types == null) {
            return null;
        }

        return carNamesQuery(carNames, (carName) -> newCarNameQueryByTypes(types, carName));
    }

    private QueryBuilder carNamesQuery(List<ForestMember> carNames, Function<ForestMember, QueryBuilder> supplier) {
        if (carNames.size() == 1) {
            return supplier.apply(carNames.get(0));
        } else {
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery().minimumNumberShouldMatch(1);
            for (ForestMember carName : carNames) {
                boolQueryBuilder.should(supplier.apply(carName));
            }

            return boolQueryBuilder;
        }
    }

    private String[] newCarsTypes(CarNameType carNameType) {
        if (carNameType == CarNameType.Variant) {
            return NEW_CAR_VARIANT_AND_UP;
        } else if (carNameType == CarNameType.Model) {
            return NEW_CAR_MODEL_AND_UP;
        } else if (carNameType == CarNameType.Brand) {
            return NEW_CAR_BRAND_AND_UP;
        }

        return null;
    }

    private QueryResponse searchNewCarsByCarName(String indexName, AutocompleteQueryRequest autocompleteQueryRequest, CarNameType carNameType, List<ForestMember> carNames) {
        return searchType(indexName,
                autocompleteQueryRequest,
                () -> this.newCarsQuery(carNameType, carNames),
                NEW_CAR_SECTION,
                () -> this.newCarsTypes(carNameType),
                SortBuilders::scoreSort,
                () -> SortBuilders.scriptSort(new Script(SORT_BY_NEW_CAR_TYPE_SCRIPT), ScriptSortBuilder.ScriptSortType.NUMBER),
                () -> SortBuilders.fieldSort(MODEL_STATUS_FIELD));
    }

    @SafeVarargs
    @Nullable
    private final QueryResponse searchType(String indexName, AutocompleteQueryRequest autocompleteQueryRequest, Supplier<QueryBuilder> querySupplier, String section, Supplier<String[]> typesSupplier, Supplier<SortBuilder>... sortSuppliers) {
        QueryBuilder query = querySupplier.get();
        if (query == null) {
            return null;
        }

        String[] types = typesSupplier.get();
        if (types == null) {
            return null;
        }

        org.elasticsearch.action.search.SearchRequestBuilder searchRequestBuilder = client.prepareSearch(indexName)
                .setTypes(types)
                .setSize(autocompleteQueryRequest.querySource().count())
                .setFrom(autocompleteQueryRequest.querySource().count() * autocompleteQueryRequest.querySource().page())
                .setQuery(query)
                .setFetchSource(INCLUDED_FIELDS, null);

        if (sortSuppliers != null) {
            for (Supplier<SortBuilder> supplier : sortSuppliers) {
                searchRequestBuilder = searchRequestBuilder.addSort(supplier.get());
            }
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Search Request: {}", searchRequestBuilder);
        }

        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();

        if (searchResponse != null && searchResponse.getHits() != null && searchResponse.getHits().getHits() != null && searchResponse.getHits().getHits().length > 0) {
            SearchHit[] hits = searchResponse.getHits().getHits();
            AutocompleteResult[] searchResults = new AutocompleteResult[hits.length];

            int i = 0;
            for (SearchHit searchHit : hits) {
                searchResults[i++] = new AutocompleteResult(searchHit);
            }

            return new AutocompleteResponse(autocompleteQueryRequest.querySource().query(), searchResults, searchResponse.getHits().getTotalHits());
        }

        return null;
    }

    private QueryBuilder simplify(BoolQueryBuilder boolQueryBuilder) {
        if (boolQueryBuilder.should().size() == 1) {
            return boolQueryBuilder.should().get(0);
        }

        return boolQueryBuilder;
    }

    private enum CarNameType {
        Brand(0),
        Model(1),
        Variant(2);

        int level;

        CarNameType(int level) {
            this.level = level;
        }

        static CarNameType fromLevel(int level) {
            if (level == 0) {
                return Brand;
            } else if (level == 1) {
                return Model;
            } else {
                return Variant;
            }
        }
    }
}

package io.threesixtyfy.humaneDiscovery.api.search;

import io.threesixtyfy.humaneDiscovery.api.commons.QueryResponse;
import io.threesixtyfy.humaneDiscovery.api.commons.TransportQueryAction;
import io.threesixtyfy.humaneDiscovery.core.cache.CacheService;
import io.threesixtyfy.humaneDiscovery.core.instance.InstanceContext;
import io.threesixtyfy.humaneDiscovery.core.tag.BaseTag;
import io.threesixtyfy.humaneDiscovery.core.tag.IntentTag;
import io.threesixtyfy.humaneDiscovery.core.tag.KeywordTag;
import io.threesixtyfy.humaneDiscovery.core.tag.NGramTag;
import io.threesixtyfy.humaneDiscovery.core.tag.TagType;
import io.threesixtyfy.humaneDiscovery.core.tagForest.ForestMember;
import io.threesixtyfy.humaneDiscovery.core.tagForest.TagForest;
import io.threesixtyfy.humaneDiscovery.core.tagForest.TagGraph;
import io.threesixtyfy.humaneDiscovery.core.tagForest.TagNode;
import io.threesixtyfy.humaneDiscovery.es.analyzer.HumaneStandardAnalyzerProvider;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
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
import org.elasticsearch.search.sort.ScriptSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;


public class TransportSearchAction extends TransportQueryAction<SearchQuerySource, SearchQueryRequest, QueryResponse> {

    private static final Logger logger = Loggers.getLogger(TransportSearchAction.class);

    private static final String USED_CAR_TYPE = "used_car";
    private static final String[] USED_CAR_TYPES = {USED_CAR_TYPE};
    private static final String CAR_NEWS_TYPE = "car_news";
    private static final String[] CAR_NEWS_TYPES = {CAR_NEWS_TYPE};
    private static final String NEW_CAR_DEALER_TYPE = "new_car_dealer";
    private static final String[] NEW_CAR_DEALER_TYPES = {NEW_CAR_DEALER_TYPE};

    private static final String NEW_CAR_SECTION = "new_car";
    private static final String USED_CAR_SECTION = "used_car";
    private static final String CAR_NEWS_SECTION = "car_news";
    private static final String NEW_CAR_DEALER_SECTION = "new_car_dealer";

    private static final String NEW_CAR_TITLE = "New Cars";
    private static final String USED_CAR_TITLE = "Used Cars";
    private static final String NEW_CAR_DEALER_TITLE = "New Car Dealers";
    private static final String CAR_NEWS_TITLE = "News";

    private static final String NEW_CAR_BRAND_TYPE = "new_car_brand";
    private static final String NEW_CAR_MODEL_PAGE_TYPE = "new_car_model_page";
    private static final String NEW_CAR_MODEL_TYPE = "new_car_model";
    private static final String NEW_CAR_VARIANT_PAGE_TYPE = "new_car_variant_page";
    private static final String NEW_CAR_VARIANT_TYPE = "new_car_variant";

    private static final String[] NEW_CAR_BRAND_AND_UP = {NEW_CAR_BRAND_TYPE, NEW_CAR_MODEL_TYPE, NEW_CAR_VARIANT_TYPE};
    private static final String[] NEW_CAR_MODEL_AND_UP = {NEW_CAR_MODEL_TYPE, NEW_CAR_VARIANT_TYPE};
    private static final String[] NEW_CAR_VARIANT_AND_UP = {NEW_CAR_VARIANT_TYPE};

    private static final String[] NEW_CAR_MODEL_AND_UP_WITH_PAGE_TYPE = {NEW_CAR_MODEL_PAGE_TYPE, NEW_CAR_MODEL_TYPE, NEW_CAR_VARIANT_TYPE};
    private static final String[] NEW_CAR_VARIANT_AND_UP_WITH_PAGE_TYPE = {NEW_CAR_VARIANT_PAGE_TYPE, NEW_CAR_VARIANT_TYPE};

    private static final String BRAND_FIELD = "brand";
    private static final String BRAND_INTENT = BRAND_FIELD;
    private static final String MODEL_FIELD = "model";
    private static final String MODEL_INTENT = MODEL_FIELD;
    private static final String VARIANT_FIELD = "variant";
    private static final String VARIANT_INTENT = VARIANT_FIELD;
    private static final String CITY_INTENT = "city";

    private static final String CITY_FIELD = "city";
    private static final String LISTING_CITY_FIELD = "listingCity";

//    private static final List<String> CAR_NAME_INTENT_FIELDS = Arrays.asList(BRAND_INTENT, MODEL_INTENT, VARIANT_INTENT);

    private static final String SORT_BY_NEW_CAR_TYPE_SCRIPT = "doc['_type'].value == 'new_car_model_page' || doc['_type'].value == 'new_car_variant_page' ? 0 : (doc['_type'].value == 'new_car_brand' ? 1 : (doc['_type'].value == 'new_car_model' ? 2 : 3))";
    private static final String DUMMY_FIELD = "dummy_field";
    private static final String[] EXCLUDED_FIELDS = {
            "colors",
            "features",
//            "pages",
            "content",
            "_weight",
            "_lang",
            "mileageInKMLRange",
            "stdExShowroomPriceInRsRange",
            "engineDisplacementInCC",
            "fuelType",
            "transmissionType",
            "seatingCapacity"
    };

    private static final String PAGETYPE = "PAGETYPE";
    private static final String PAGE_TYPE_FIELD = "pageType";
    private static final String PAGE_NAME_FIELD = "name";
    private static final String MODEL_STATUS_FIELD = "modelStatus";

    private static Map<String, String> SectionTitles = new HashMap<>();
    private static Map<String, String> NewCarFields = new HashMap<>();

    static {
        SectionTitles.put(NEW_CAR_SECTION, NEW_CAR_TITLE);
        SectionTitles.put(USED_CAR_SECTION, USED_CAR_TITLE);
        SectionTitles.put(CAR_NEWS_SECTION, CAR_NEWS_TITLE);
        SectionTitles.put(NEW_CAR_DEALER_SECTION, NEW_CAR_DEALER_TITLE);
    }

    static {
        NewCarFields.put(NEW_CAR_BRAND_TYPE, BRAND_FIELD);
        NewCarFields.put(NEW_CAR_MODEL_TYPE, MODEL_FIELD);
        NewCarFields.put(NEW_CAR_VARIANT_TYPE, VARIANT_FIELD);
    }

    private final IndicesOptions indicesOptions = IndicesOptions.strictExpandOpenAndForbidClosed();

    private final Analyzer analyzer;

    @Inject
    public TransportSearchAction(Settings settings,
                                 ThreadPool threadPool,
                                 TransportService transportService,
                                 ActionFilters actionFilters,
                                 IndexNameExpressionResolver indexNameExpressionResolver,
                                 ClusterService clusterService,
                                 IndicesService indicesService,
                                 Client client,
                                 CacheService cacheService) throws IOException {
        super(settings, SearchAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, clusterService, indicesService, client, SearchQueryRequest::new, cacheService);

        this.analyzer = indicesService.getAnalysis().getAnalyzer(HumaneStandardAnalyzerProvider.NAME);
    }

    protected QueryResponse response(SearchQueryRequest queryRequest) {
        return this.cacheService.getOrCompute(queryRequest.key(), QueryResponse.class, () -> {
            QueryResponse queryResponse = null;
            Index[] indices = indexNameExpressionResolver.concreteIndices(clusterService.state(), indicesOptions, StringUtils.lowerCase(queryRequest.instance()) + "_store");

            InstanceContext instanceContext = this.instanceContexts.get(queryRequest.instance());

            // TODO: support type specific searches
            if (instanceContext != null && indices != null && indices.length == 1) {
                Index index = indices[0];

                List<TagForest> tagForests = createIntents(queryRequest, instanceContext);
                if (tagForests != null && tagForests.size() > 0) {
                    queryResponse = buildSearch(index.getName(), queryRequest.querySource().section(), queryRequest, tagForests.get(0));
                }
            }

            queryResponse = queryResponse == null ? new SingleSectionSearchResponse(queryRequest.querySource().query()) : queryResponse;

            return queryResponse;
        });
    }

    // dealer section will come by car name or dealer name only, can be filtered by city
    // news section will also come by car name only
    private QueryResponse buildSearch(String indexName, String section, SearchQueryRequest searchQueryRequest, TagForest tagForest) {
//        if (dealerNameSearch(tagForest)) {
//            // build only dealer section
//        }
//
//        if (dealerSearch(tagForest)) {
//            // build only dealer section
//            if (isCitySearch(tagForest)) {
//                // show dealers for the city
//            }
//        }
//
//        if (usedCarSearch(tagForest)) {
//            // build only used car section
//            if (isCitySearch(tagForest)) {
//                // show used cars for the city
//            }
//        }
//
        // here we do all type of searches
        if (isCarNameSearch(tagForest)) {
            // extract page type
            // car name - if car name is not there, we treat it as normal search
            // prepend page type search result to all
            String pageType = extractPageType(tagForest);
//            if (isPageTypeSearch(tagForest)) {
//
//                pageType = extractPageType(tagForest);
//            }

            // when by car name...
            // if brand => then new_car_brand, new_car_model, new_car_variant - all 3 active
            // if model => then new_car_model, new_car_variant active
            // if variant => then new_car_variant active
            List<ForestMember> carNames = new ArrayList<>();
            CarNameType carNameType = extractCarNames(tagForest, carNames::add);

            if (section != null) {
                SectionResult sectionResult = null;
                if (StringUtils.equals(section, NEW_CAR_SECTION)) {
                    sectionResult = this.searchNewCarsByCarName(indexName, searchQueryRequest, carNameType, carNames, pageType);
                } else if (StringUtils.equals(section, USED_CAR_SECTION)) {
                    sectionResult = this.searchUsedCarsByCarName(indexName, searchQueryRequest, carNameType, carNames);
                } else if (StringUtils.equals(section, CAR_NEWS_SECTION)) {
                    sectionResult = this.searchNewsByCarName(indexName, searchQueryRequest, carNameType, carNames);
                } else if (StringUtils.equals(section, NEW_CAR_DEALER_SECTION)) {
                    sectionResult = this.searchDealersByCarName(indexName, searchQueryRequest, carNameType, carNames);
                }

                if (sectionResult != null) {
                    return new SingleSectionSearchResponse(searchQueryRequest.querySource().query(), sectionResult);
                }
            } else {

                SectionResult newCarsResult = searchNewCarsByCarName(indexName, searchQueryRequest, carNameType, carNames, pageType);
                SectionResult usedCarsResult = searchUsedCarsByCarName(indexName, searchQueryRequest, carNameType, carNames);
                SectionResult carNewsResult = searchNewsByCarName(indexName, searchQueryRequest, carNameType, carNames);
                SectionResult dealersResult = searchDealersByCarName(indexName, searchQueryRequest, carNameType, carNames);

                return new MultiSectionSearchResponse(searchQueryRequest.querySource().query(), filterNull(newCarsResult, usedCarsResult, carNewsResult, dealersResult));
            }
        } else if (isUsedCarSearch(tagForest)) {

        } else if (isDealerSearch(tagForest)) {

        } else if (isCitySearch(tagForest)) {
            // this would be mostly used cars or dealers only
            List<ForestMember> cityNames = new ArrayList<>();
            extractCity(tagForest, cityNames::add);

            if (section != null) {
                SectionResult sectionResult = null;
                if (StringUtils.equals(section, USED_CAR_SECTION)) {
                    sectionResult = this.searchUsedCarsByCity(indexName, searchQueryRequest, cityNames);
                } else if (StringUtils.equals(section, NEW_CAR_DEALER_SECTION)) {
                    sectionResult = this.searchDealersByCity(indexName, searchQueryRequest, cityNames);
                }

                if (sectionResult != null) {
                    return new SingleSectionSearchResponse(searchQueryRequest.querySource().query(), sectionResult);
                }
            } else {
                SectionResult usedCarsResult = searchUsedCarsByCity(indexName, searchQueryRequest, cityNames);
                SectionResult dealersResult = searchDealersByCity(indexName, searchQueryRequest, cityNames);
                return new MultiSectionSearchResponse(searchQueryRequest.querySource().query(), filterNull(usedCarsResult, dealersResult));
            }
        }

        // over car name, optional filters such as car type, fuel type, seating capacity, automatic transmission can be applied
        // when such features are needed => brand shall not be active
        return null;
    }

    private boolean isKeyword(TagForest tagForest, boolean excludeNgram, String... keywordNames) {
        for (ForestMember forestMember : tagForest.getMembers()) {
            for (BaseTag tag : forestMember.getTags()) {
                if ((tag.getTagType() == TagType.Keyword || !excludeNgram && tag instanceof NGramTag && ((NGramTag) tag).getRefTagType() == TagType.Keyword)
                        && Arrays.asList(keywordNames).contains(tag.getName())) {
                    return true;
                }

            }
        }

        return false;
    }

    private boolean isUsedCarSearch(TagForest tagForest) {
        return false;
    }


    private boolean isDealerSearch(TagForest tagForest) {
        return false;
    }

    private boolean isIntent(TagForest tagForest, boolean excludeNgram, String... intentFields) {
        for (ForestMember forestMember : tagForest.getMembers()) {
            for (BaseTag tag : forestMember.getTags()) {
                if ((tag.getTagType() == TagType.Intent || !excludeNgram && tag instanceof NGramTag && ((NGramTag) tag).getRefTagType() == TagType.Intent)
                        && Arrays.asList(intentFields).contains(tag.getName())) {
                    return true;
                }

            }
        }

        return false;
    }

    private boolean isCitySearch(TagForest tagForest) {
        return isIntent(tagForest, false, CITY_INTENT);
    }

    private boolean isCarNameSearch(TagForest tagForest) {
        // if there is a forest member with intent tag of brand / model / variant type OR NGRAM tag with ref to Intent of brand / model / variant
        return isIntent(tagForest, false, BRAND_INTENT, MODEL_INTENT, VARIANT_INTENT);
    }

    //    private boolean dealerNameSearch(TagForest tagForest) {
//        return false;
//    }

    private String extractPageType(TagForest tagForest) {
        for (ForestMember forestMember : tagForest.getMembers()) {
            for (BaseTag tag : forestMember.getTags()) {
                if (tag instanceof KeywordTag) {
                    KeywordTag keywordTag = (KeywordTag) tag;

                    if (StringUtils.equals(keywordTag.getName(), PAGETYPE)) {
                        return StringUtils.lowerCase(keywordTag.getNormalisedValue());
                    }
                }
            }
        }

        return null;
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

                    if (nGramTag.getRefTagType() == TagType.Intent) {
                        if (StringUtils.equals(nGramTag.getName(), BRAND_INTENT)) {
                            hasBrand = true;
                        } else if (StringUtils.equals(nGramTag.getName(), MODEL_INTENT)) {
                            hasModel = true;
                        } else if (StringUtils.equals(nGramTag.getName(), VARIANT_INTENT)) {
                            hasVariant = true;
                        }
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

    private void extractCity(TagForest tagForest, Consumer<ForestMember> consumer) {
        for (ForestMember forestMember : tagForest.getMembers()) {

            boolean hasCity = false;
            for (BaseTag tag : forestMember.getTags()) {
                if (tag instanceof IntentTag) {
                    IntentTag intentTag = (IntentTag) tag;

                    if (StringUtils.equals(intentTag.getName(), CITY_INTENT)) {
                        hasCity = true;
                    }
                } else if (tag instanceof NGramTag) {
                    NGramTag nGramTag = (NGramTag) tag;

                    if (nGramTag.getRefTagType() == TagType.Intent) {
                        if (StringUtils.equals(nGramTag.getName(), CITY_INTENT)) {
                            hasCity = true;
                        }
                    }
                }
            }

            if (hasCity) {
                consumer.accept(forestMember);
            }
        }
    }

//    private void extractVehicleType(TagForest tagForest) {
//
//    }
//
//    private void extractFuelType(TagForest tagForest) {
//
//    }
//
//    private void extractSeat(TagForest tagForest) {
//
//    }
//
//    private void extractTransmission(TagForest tagForest) {
//
//    }
//
//    private void extractDealerName(TagForest tagForest) {
//
//    }

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

    private QueryBuilder fieldQueryWithAnalysis(String field, String text) throws IOException {
        TokenStream tokenStream = this.analyzer.tokenStream(DUMMY_FIELD, text);

        tokenStream.reset();
        CharTermAttribute termAttribute = tokenStream.getAttribute(CharTermAttribute.class);

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        while (tokenStream.incrementToken()) {
            String word = termAttribute.toString();
            boolQueryBuilder.must(fieldQuery(field, word));
        }

        tokenStream.close();

        return boolQueryBuilder;
    }

    private QueryBuilder newCarNameQueryByType(String type, ForestMember forestMember) {
        String field = NewCarFields.get(type);

        QueryBuilder queryBuilder = fieldQuery(field, forestMember);
        if (queryBuilder == null) {
            return null;
        }

        return QueryBuilders.boolQuery().must(queryBuilder).filter(QueryBuilders.typeQuery(type));
    }

    private QueryBuilder newCarNameQueryForPageType(String type, ForestMember forestMember, String pageType) {
        QueryBuilder queryBuilder = fieldQuery(PAGE_NAME_FIELD, forestMember);
        if (queryBuilder == null) {
            return null;
        }

        return QueryBuilders.boolQuery().must(queryBuilder).filter(QueryBuilders.termQuery(PAGE_TYPE_FIELD, pageType)).filter(QueryBuilders.typeQuery(type));
    }

    private QueryBuilder newCarNameQueryByTypes(String[] types, ForestMember carName, String pageType) {
        if (types.length == 1) {
            return newCarNameQueryByType(types[0], carName);
        } else {
            DisMaxQueryBuilder disMaxQueryBuilder = QueryBuilders.disMaxQuery();
            for (String type : types) {
                QueryBuilder typeQuery = pageType != null && (StringUtils.equals(type, NEW_CAR_MODEL_PAGE_TYPE) || StringUtils.equals(type, NEW_CAR_VARIANT_PAGE_TYPE))
                        ? newCarNameQueryForPageType(type, carName, pageType)
                        : newCarNameQueryByType(type, carName);
                if (typeQuery != null) {
                    disMaxQueryBuilder.add(typeQuery);
                }
            }

            return disMaxQueryBuilder;
        }
    }

    // TODO: do we relax queries in some ways
    private QueryBuilder newCarsQuery(CarNameType carNameType, List<ForestMember> carNames, String pageType) {
        // if it has variant part, then we form variant query and optional model / brand in new_car_variant
        // if it has model part, then we form model query and optional brand in new_car_model and new_car_variant
        // if it has brand query, then we form brand query in new_car_brand, new_car_model, new_car_variant
        if (carNames == null || carNames.size() == 0) {
            return null;
        }

        String[] types = newCarsTypes(carNameType, pageType);
        if (types == null) {
            return null;
        }

        return fieldQuery(carNames, (carName) -> newCarNameQueryByTypes(types, carName, pageType));
    }

    // TODO: relax queries in some ways... say relaxing by model, and then brand... not so important
    private QueryBuilder usedCarsQueryByCarNames(CarNameType carNameType, List<ForestMember> carNames) {
        return fieldQuery(carNames, (carName) -> fieldQuery(VARIANT_FIELD, carName));
    }

    private QueryBuilder usedCarsQueryByCity(List<ForestMember> cityNames) {
        return fieldQuery(cityNames, (carName) -> fieldQuery(LISTING_CITY_FIELD, carName));
    }

    private QueryBuilder fieldQuery(List<ForestMember> carNames, Function<ForestMember, QueryBuilder> supplier) {
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

    private QueryBuilder fieldQuery(String field, Supplier<Set<String>> supplier) {
        Set<String> names = supplier.get();

        if (!names.isEmpty()) {
            // make query here
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery().minimumNumberShouldMatch(1);
            for (String name : names) {
                try {
                    boolQueryBuilder.should(fieldQueryWithAnalysis(field, name));
                } catch (IOException e) {
                    return null;
                }
            }

            return simplify(boolQueryBuilder);
        }

        return null;
    }

    private QueryBuilder carNewsQuery(CarNameType carNameType, List<ForestMember> carNames) {
        // here also we first try to search by variant
        // then we search by model
        // then we search by brand
        if (carNameType == CarNameType.Variant) {
            // search for variant
            // search for model
            // search for brand
            QueryBuilder variantQuery = fieldQuery(carNames, (carName) -> fieldQuery(VARIANT_FIELD, carName));
            QueryBuilder modelQuery = fieldQuery(MODEL_FIELD, () -> extractModels(carNames));
            QueryBuilder brandQuery = fieldQuery(BRAND_FIELD, () -> extractBrands(carNames));

            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery().minimumNumberShouldMatch(1);
            if (variantQuery != null) {
                boolQueryBuilder.should(variantQuery);
            }

            if (modelQuery != null) {
                boolQueryBuilder.should(modelQuery.boost(0.50f));
            }

            if (brandQuery != null) {
                boolQueryBuilder.should(brandQuery.boost(0.25f));
            }

            return simplify(boolQueryBuilder);
        } else if (carNameType == CarNameType.Model) {
            // search for model
            // search for brand
            QueryBuilder modelQuery = fieldQuery(carNames, (carName) -> fieldQuery(MODEL_FIELD, carName));
            QueryBuilder brandQuery = fieldQuery(BRAND_FIELD, () -> extractBrands(carNames));

            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery().minimumNumberShouldMatch(1);
            if (modelQuery != null) {
                boolQueryBuilder.should(modelQuery);
            }

            if (brandQuery != null) {
                boolQueryBuilder.should(brandQuery.boost(0.50f));
            }

            return simplify(boolQueryBuilder);
        } else {
            // search for brand
            return fieldQuery(carNames, (carName) -> fieldQuery(BRAND_FIELD, carName));
        }
    }

    private QueryBuilder carDealersQueryByCarNames(CarNameType carNameType, List<ForestMember> carNames) {
        if (carNameType == CarNameType.Brand) {
            return fieldQuery(carNames, (name) -> fieldQuery(BRAND_FIELD, name));
        } else {
            return fieldQuery(BRAND_FIELD, () -> extractBrands(carNames));
        }
    }

    private QueryBuilder carDealersQueryByCity(List<ForestMember> cityNames) {
        return fieldQuery(cityNames, (name) -> fieldQuery(CITY_FIELD, name));
    }

    @NotNull
    private Set<String> extractBrands(List<ForestMember> carNames) {
        Set<String> brands = new HashSet<>();

        // extract brands
        for (ForestMember carName : carNames) {
            for (BaseTag tag : carName.getTags()) {
                if (tag instanceof IntentTag) {
                    IntentTag intentTag = (IntentTag) tag;

                    if (StringUtils.equals(intentTag.getName(), MODEL_INTENT)) {
                        if (intentTag.getAncestors() != null && intentTag.getAncestors().containsKey(BRAND_INTENT)) {
                            brands.addAll(intentTag.getAncestors().get(BRAND_INTENT));
                        }
                    } else if (StringUtils.equals(intentTag.getName(), VARIANT_INTENT)) {
                        if (intentTag.getAncestors() != null && intentTag.getAncestors().containsKey(BRAND_INTENT)) {
                            brands.addAll(intentTag.getAncestors().get(BRAND_INTENT));
                        }
                    }
                } else if (tag instanceof NGramTag) {
                    NGramTag nGramTag = (NGramTag) tag;

                    if (nGramTag.getRefTagType() == TagType.Intent && StringUtils.equals(nGramTag.getName(), BRAND_INTENT)) {
                        if (nGramTag.getAncestors() != null && nGramTag.getAncestors().containsKey(BRAND_INTENT)) {
                            brands.addAll(nGramTag.getAncestors().get(BRAND_INTENT));
                        }
                    } else if (nGramTag.getRefTagType() == TagType.Intent && StringUtils.equals(nGramTag.getName(), MODEL_INTENT)) {
                        if (nGramTag.getAncestors() != null && nGramTag.getAncestors().containsKey(BRAND_INTENT)) {
                            brands.addAll(nGramTag.getAncestors().get(BRAND_INTENT));
                        }
                    } else if (nGramTag.getRefTagType() == TagType.Intent && StringUtils.equals(nGramTag.getName(), VARIANT_INTENT)) {
                        if (nGramTag.getAncestors() != null && nGramTag.getAncestors().containsKey(BRAND_INTENT)) {
                            brands.addAll(nGramTag.getAncestors().get(BRAND_INTENT));
                        }
                    }
                }
            }
        }

        return brands;
    }

    @NotNull
    private Set<String> extractModels(List<ForestMember> carNames) {
        Set<String> models = new HashSet<>();

        // extract brands
        for (ForestMember carName : carNames) {
            for (BaseTag tag : carName.getTags()) {
                if (tag instanceof IntentTag) {
                    IntentTag intentTag = (IntentTag) tag;

                    if (StringUtils.equals(intentTag.getName(), VARIANT_INTENT)) {
                        if (intentTag.getAncestors() != null && intentTag.getAncestors().containsKey(MODEL_INTENT)) {
                            models.addAll(intentTag.getAncestors().get(MODEL_INTENT));
                        }
                    }
                } else if (tag instanceof NGramTag) {
                    NGramTag nGramTag = (NGramTag) tag;

                    if (nGramTag.getRefTagType() == TagType.Intent && StringUtils.equals(nGramTag.getName(), MODEL_INTENT)) {
                        if (nGramTag.getAncestors() != null && nGramTag.getAncestors().containsKey(MODEL_INTENT)) {
                            models.addAll(nGramTag.getAncestors().get(MODEL_INTENT));
                        }
                    } else if (nGramTag.getRefTagType() == TagType.Intent && StringUtils.equals(nGramTag.getName(), VARIANT_INTENT)) {
                        if (nGramTag.getAncestors() != null && nGramTag.getAncestors().containsKey(MODEL_INTENT)) {
                            models.addAll(nGramTag.getAncestors().get(MODEL_INTENT));
                        }
                    }
                }
            }
        }

        return models;
    }

    private String[] newCarsTypes(CarNameType carNameType, String pageType) {
        if (carNameType == CarNameType.Variant) {
            if (pageType != null) {
                return NEW_CAR_VARIANT_AND_UP_WITH_PAGE_TYPE;
            }

            return NEW_CAR_VARIANT_AND_UP;
        } else if (carNameType == CarNameType.Model) {
            if (pageType != null) {
                return NEW_CAR_MODEL_AND_UP_WITH_PAGE_TYPE;
            }

            return NEW_CAR_MODEL_AND_UP;
        } else if (carNameType == CarNameType.Brand) {
            return NEW_CAR_BRAND_AND_UP;
        }

        return null;
    }

    private SectionResult searchNewCarsByCarName(String indexName, SearchQueryRequest searchQueryRequest, CarNameType carNameType, List<ForestMember> carNames, String pageType) {
        return searchType(indexName,
                searchQueryRequest,
                () -> this.newCarsQuery(carNameType, carNames, pageType),
                NEW_CAR_SECTION,
                () -> this.newCarsTypes(carNameType, pageType),
                SortBuilders::scoreSort,
                () -> SortBuilders.scriptSort(new Script(SORT_BY_NEW_CAR_TYPE_SCRIPT), ScriptSortBuilder.ScriptSortType.NUMBER),
                () -> SortBuilders.fieldSort(MODEL_STATUS_FIELD));
    }

    private SectionResult searchUsedCarsByCarName(String indexName, SearchQueryRequest searchQueryRequest, CarNameType carNameType, List<ForestMember> carNames) {
        return searchType(indexName,
                searchQueryRequest,
                () -> this.usedCarsQueryByCarNames(carNameType, carNames),
                USED_CAR_SECTION,
                () -> USED_CAR_TYPES);
    }

    private SectionResult searchNewsByCarName(String indexName, SearchQueryRequest searchQueryRequest, CarNameType carNameType, List<ForestMember> carNames) {
        return searchType(indexName,
                searchQueryRequest,
                () -> this.carNewsQuery(carNameType, carNames),
                CAR_NEWS_SECTION,
                () -> CAR_NEWS_TYPES);
    }

    private SectionResult searchDealersByCarName(String indexName, SearchQueryRequest searchQueryRequest, CarNameType carNameType, List<ForestMember> carNames) {
        return searchType(indexName,
                searchQueryRequest,
                () -> this.carDealersQueryByCarNames(carNameType, carNames),
                NEW_CAR_DEALER_SECTION,
                () -> NEW_CAR_DEALER_TYPES);
    }

    private SectionResult searchUsedCarsByCity(String indexName, SearchQueryRequest searchQueryRequest, List<ForestMember> cityNames) {
        return searchType(indexName,
                searchQueryRequest,
                () -> this.usedCarsQueryByCity(cityNames),
                USED_CAR_SECTION,
                () -> USED_CAR_TYPES);
    }

    private SectionResult searchDealersByCity(String indexName, SearchQueryRequest searchQueryRequest, List<ForestMember> cityNames) {
        return searchType(indexName,
                searchQueryRequest,
                () -> this.carDealersQueryByCity(cityNames),
                NEW_CAR_DEALER_SECTION,
                () -> NEW_CAR_DEALER_TYPES);
    }

    @SafeVarargs
    @Nullable
    private final SectionResult searchType(String indexName, SearchQueryRequest searchQueryRequest, Supplier<QueryBuilder> querySupplier, String section, Supplier<String[]> typesSupplier, Supplier<SortBuilder>... sortSuppliers) {
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
                .setSize(searchQueryRequest.querySource().count())
                .setFrom(searchQueryRequest.querySource().count() * searchQueryRequest.querySource().page())
                .setQuery(query)
                .setFetchSource(null, EXCLUDED_FIELDS);

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
            return new SectionResult(section, SectionTitles.get(section), section, searchResponse.getHits().getHits(), searchResponse.getHits().getTotalHits());
        }

        return null;
    }

    private SectionResult[] filterNull(SectionResult... sectionResults) {
        return Arrays.stream(sectionResults).filter(Objects::nonNull).toArray(SectionResult[]::new);
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

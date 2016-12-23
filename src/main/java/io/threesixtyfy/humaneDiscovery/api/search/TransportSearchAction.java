package io.threesixtyfy.humaneDiscovery.api.search;

import io.threesixtyfy.humaneDiscovery.api.commons.QueryResponse;
import io.threesixtyfy.humaneDiscovery.api.commons.TransportQueryAction;
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

    private static final List<String> CAR_NAME_INTENT_FIELDS = Arrays.asList(BRAND_INTENT, MODEL_INTENT, VARIANT_INTENT);

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
                                 Client client) throws IOException {
        super(settings, SearchAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, clusterService, indicesService, client, SearchQueryRequest::new);

        this.analyzer = indicesService.getAnalysis().getAnalyzer(HumaneStandardAnalyzerProvider.NAME);
    }

    protected QueryResponse response(SearchQueryRequest searchQueryRequest) throws IOException {
        Index[] indices = indexNameExpressionResolver.concreteIndices(clusterService.state(), indicesOptions, StringUtils.lowerCase(searchQueryRequest.instance()) + "_store");

        // TODO: depending on input type / section, fetch tag scopes
        // TODO: support type specific searches
        QueryResponse queryResponse = null;
        if (indices != null && indices.length == 1) {
            Index index = indices[0];

            List<TagForest> tagForests = createIntents(searchQueryRequest);
            if (tagForests != null && tagForests.size() > 0) {
                queryResponse = buildSearch(index.getName(), searchQueryRequest.querySource().section(), searchQueryRequest, tagForests.get(0));
            }

//            String section = searchQueryRequest.querySource().section();
//            String type = searchQueryRequest.querySource().type();
//            if (section == null && type == null) {
//                SectionResult[] sectionResults = new SectionResult[]{
//                        this.searchNewCars(index.getName(), searchQueryRequest),
//                        this.searchUsedCars(index.getName(), searchQueryRequest),
//                        this.searchNews(index.getName(), searchQueryRequest),
//                        this.searchDealers(index.getName(), searchQueryRequest)
//                };
//
//                return new MultiSectionSearchResponse(sectionResults, 0);
//            } else if (section != null) {
//                SectionResult sectionResult = null;
//                if (StringUtils.equals(section, NEW_CAR_TYPE)) {
//                    sectionResult = this.searchNewCars(index.getName(), searchQueryRequest);
//                } else if (StringUtils.equals(section, USED_CAR_TYPE)) {
//                    sectionResult = this.searchUsedCars(index.getName(), searchQueryRequest);
//                } else if (StringUtils.equals(section, CAR_NEWS_TYPE)) {
//                    sectionResult = this.searchNews(index.getName(), searchQueryRequest);
//                } else if (StringUtils.equals(section, NEW_CAR_DEALER_TYPE)) {
//                    sectionResult = this.searchDealers(index.getName(), searchQueryRequest);
//                }
//
//                if (sectionResult != null) {
//                    return new AutocompleteResponse(sectionResult, 0);
//                }
//            }
        }

        return queryResponse == null ? new SingleSectionSearchResponse(searchQueryRequest.querySource().query()) : queryResponse;
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
//            if (citySearch(tagForest)) {
//                // show dealers for the city
//            }
//        }
//
//        if (usedCarSearch(tagForest)) {
//            // build only used car section
//            if (citySearch(tagForest)) {
//                // show used cars for the city
//            }
//        }
//
//        if (citySearch(tagForest)) {
//            // this would be mostly used cars or dealers only
//        }

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
        }

        // over car name, optional filters such as car type, fuel type, seating capacity, automatic transmission can be applied
        // when such features are needed => brand shall not be active
        return null;
    }

//    private boolean usedCarSearch(TagForest tagForest) {
//        return false;
//    }
//
//    private boolean dealerSearch(TagForest tagForest) {
//        return false;
//    }
//
//    private boolean citySearch(TagForest tagForest) {
//        return false;
//    }
//
//    private boolean dealerNameSearch(TagForest tagForest) {
//        return false;
//    }

    private boolean isCarNameSearch(TagForest tagForest) {
        // if there is a forest member with intent tag of brand / model / variant type OR NGRAM tag with ref to Intent of brand / model / variant
        for (ForestMember forestMember : tagForest.getMembers()) {
            for (BaseTag tag : forestMember.getTags()) {
                if (tag instanceof IntentTag) {
                    IntentTag intentTag = (IntentTag) tag;

                    if (CAR_NAME_INTENT_FIELDS.contains(intentTag.getName())) {
                        return true;
                    }
                } else if (tag instanceof NGramTag) {
                    NGramTag nGramTag = (NGramTag) tag;

                    if (nGramTag.getRefTagType() == TagType.Intent && CAR_NAME_INTENT_FIELDS.contains(nGramTag.getName())) {
                        return true;
                    }
                }
            }
        }

        return false;
    }

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
//    private void extractCity(TagForest tagForest) {
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
        QueryBuilder queryBuilder = fieldQuery("name", forestMember);
        if (queryBuilder == null) {
            return null;
        }

        return QueryBuilders.boolQuery().must(queryBuilder).filter(QueryBuilders.termQuery("pageType", pageType)).filter(QueryBuilders.typeQuery(type));
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

        return carNamesQuery(carNames, (carName) -> newCarNameQueryByTypes(types, carName, pageType));
    }

    // TODO: relax queries in some ways... say relaxing by model, and then brand... not so important
    private QueryBuilder usedCarsQuery(CarNameType carNameType, List<ForestMember> carNames) {
        return carNamesQuery(carNames, (carName) -> fieldQuery(VARIANT_FIELD, carName));
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

    private QueryBuilder carNamesQuery(String field, Supplier<Set<String>> supplier) {
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
            QueryBuilder variantQuery = carNamesQuery(carNames, (carName) -> fieldQuery(VARIANT_FIELD, carName));
            QueryBuilder modelQuery = carNamesQuery(MODEL_FIELD, () -> extractModels(carNames));
            QueryBuilder brandQuery = carNamesQuery(BRAND_FIELD, () -> extractBrands(carNames));

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
            QueryBuilder modelQuery = carNamesQuery(carNames, (carName) -> fieldQuery(MODEL_FIELD, carName));
            QueryBuilder brandQuery = carNamesQuery(BRAND_FIELD, () -> extractBrands(carNames));

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
            return carNamesQuery(carNames, (carName) -> fieldQuery(BRAND_FIELD, carName));
        }
    }

    private QueryBuilder carDealersQuery(CarNameType carNameType, List<ForestMember> carNames) {
        if (carNameType == CarNameType.Brand) {
            return carNamesQuery(carNames, (carName) -> fieldQuery(BRAND_FIELD, carName));
        } else {
            return carNamesQuery(BRAND_FIELD, () -> extractBrands(carNames));
        }
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
                        if (intentTag.getAncestors() != null && intentTag.getAncestors().size() == 1) {
                            brands.addAll(intentTag.getAncestors().get(0));
                        }
                    } else if (StringUtils.equals(intentTag.getName(), VARIANT_INTENT)) {
                        if (intentTag.getAncestors() != null && intentTag.getAncestors().size() == 2) {
                            brands.addAll(intentTag.getAncestors().get(1));
                        }
                    }
                } else if (tag instanceof NGramTag) {
                    NGramTag nGramTag = (NGramTag) tag;

                    if (nGramTag.getRefTagType() == TagType.Intent && StringUtils.equals(nGramTag.getName(), BRAND_INTENT)) {
                        if (nGramTag.getAncestors() != null && nGramTag.getAncestors().size() == 1) {
                            brands.addAll(nGramTag.getAncestors().get(0));
                        }
                    } else if (nGramTag.getRefTagType() == TagType.Intent && StringUtils.equals(nGramTag.getName(), MODEL_INTENT)) {
                        if (nGramTag.getAncestors() != null && nGramTag.getAncestors().size() == 2) {
                            brands.addAll(nGramTag.getAncestors().get(1));
                        }
                    } else if (nGramTag.getRefTagType() == TagType.Intent && StringUtils.equals(nGramTag.getName(), VARIANT_INTENT)) {
                        if (nGramTag.getAncestors() != null && nGramTag.getAncestors().size() == 3) {
                            brands.addAll(nGramTag.getAncestors().get(2));
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
                        if (intentTag.getAncestors() != null && intentTag.getAncestors().size() == 2) {
                            models.addAll(intentTag.getAncestors().get(0));
                        }
                    }
                } else if (tag instanceof NGramTag) {
                    NGramTag nGramTag = (NGramTag) tag;

                    if (nGramTag.getRefTagType() == TagType.Intent && StringUtils.equals(nGramTag.getName(), MODEL_INTENT)) {
                        if (nGramTag.getAncestors() != null && nGramTag.getAncestors().size() == 2) {
                            models.addAll(nGramTag.getAncestors().get(0));
                        }
                    } else if (nGramTag.getRefTagType() == TagType.Intent && StringUtils.equals(nGramTag.getName(), VARIANT_INTENT)) {
                        if (nGramTag.getAncestors() != null && nGramTag.getAncestors().size() == 3) {
                            models.addAll(nGramTag.getAncestors().get(1));
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
                () -> SortBuilders.scriptSort(new Script(SORT_BY_NEW_CAR_TYPE_SCRIPT), ScriptSortBuilder.ScriptSortType.NUMBER));
    }

    private SectionResult searchUsedCarsByCarName(String indexName, SearchQueryRequest searchQueryRequest, CarNameType carNameType, List<ForestMember> carNames) {
        return searchType(indexName,
                searchQueryRequest,
                () -> this.usedCarsQuery(carNameType, carNames),
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
                () -> this.carDealersQuery(carNameType, carNames),
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

package io.threesixtyfy.humaneDiscovery.es.query;

import io.threesixtyfy.humaneDiscovery.es.analyzer.HumaneEdgeGramQueryAnalyzerProvider;
import io.threesixtyfy.humaneDiscovery.es.analyzer.HumaneKeywordAnalyzerProvider;
import io.threesixtyfy.humaneDiscovery.es.analyzer.HumaneQueryAnalyzerProvider;
import io.threesixtyfy.humaneDiscovery.core.conjuncts.DisjunctsBuilder;
import io.threesixtyfy.humaneDiscovery.core.conjuncts.TokensBuilder;
import io.threesixtyfy.humaneDiscovery.core.tagger.TagBuilder;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.QueryBuilder;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public abstract class BaseHumaneQueryBuilder<T extends BaseHumaneQueryBuilder<T>> extends AbstractQueryBuilder<T> {

    protected static final ParseField QUERY_FIELD = new ParseField("query");
    protected static final ParseField NO_FUZZY_FIELD = new ParseField("noFuzzy");
    protected static final ParseField KEYWORD_FIELD = new ParseField("keyword");
    protected static final ParseField VERNACULAR_ONLY_FIELD = new ParseField("vernacularOnly");
    protected static final ParseField INSTANCE_FIELD = new ParseField("instance");
    protected static final ParseField INTENT_FIELDS_FIELD = new ParseField("intentFields");
    private static final Logger logger = Loggers.getLogger(BaseHumaneQueryBuilder.class);
    private static final String MATCHING_NO_DOCS_REASON = "No matching docs";

    private static final float TIE_BREAKER_MULTIPLIER_DEFAULT = 1.0f;
    private static final float EXACT_MATCH_BOOST = 100.0f;
    private static final float SYNONYM_MATCH_BOOST = 80.0f;
    private static final float EDGE_GRAM_MATCH_BOOST = 20.0f;
    //    private static final float SHINGLE_EXACT_MATCH_BOOST = 200.0f;
    private static final float PHONETIC_BOOST = 5.0f;
    //    private static final float SHINGLE_EDGE_GRAM_BOOST = 40.0f;
    private static final int MIN_NO_FUZZY_TOKEN_LENGTH = 2;
    private static final String SHINGLE_FIELD_SUFFIX = ".shingle";
    private static final String HUMANE_FIELD_SUFFIX = ".humane";
    private static final String SEARCH_QUERY_STORE_PREFIX = ":search_query_store";
    private static final String SEARCH_QUERY_STORE_SUFFIX = SEARCH_QUERY_STORE_PREFIX;
    private static final float EDIT_DISTANCE_NORMALIZE_FACTOR = 10.0f;
    //    private static final Map<String, QueryTag> EmptySuggestionMap = new HashMap<>();
    private static final String STANDARD_QUERY_ANALYZER_NAME = HumaneQueryAnalyzerProvider.NAME;
    private static final String STANDARD_EDGE_GRAM_QUERY_ANALYZER_NAME = HumaneEdgeGramQueryAnalyzerProvider.NAME;
    private static final String KEYWORD_ANALYZER_NAME = HumaneKeywordAnalyzerProvider.NAME;

    private final Map<String, QueryBuilder> QUERY_BUILDER_CACHE = new HashMap<>();
    private static final Map<String, NestedPathContext> NESTED_PATH_CONTEXT_CACHE = new HashMap<>();

    private static final TagBuilder TAG_BUILDER = TagBuilder.INSTANCE();
    private static final TokensBuilder TOKENS_BUILDER = TokensBuilder.INSTANCE();
    private static final DisjunctsBuilder DISJUNCTS_BUILDER = DisjunctsBuilder.INSTANCE();

    protected String queryText;
    protected String instance;
    protected Set<String> intentFields;

    protected BaseHumaneQueryBuilder() {
        super();
    }

    protected BaseHumaneQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.queryText = in.readString();
        this.instance = in.readString();

        int intentFieldsCount = in.readInt();
        this.intentFields = new HashSet<>(intentFieldsCount);

        for (int i = 0; i < intentFieldsCount; i++) {
            this.intentFields.add(in.readString());
        }
    }

    protected static void parseIntentFields(XContentParser parser, XContentParser.Token token, Set<String> intentFields) throws IOException {
        if (token == XContentParser.Token.START_ARRAY) {
            // parseObject fields
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                if (token.isValue()) {
                    // we have simple field
                    intentFields.add(parser.text());
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "intent field must be simple field name but got [" + token + "]");
                }
            }
        } else {
            throw new ParsingException(parser.getTokenLocation(), "intentFields must be array of fields, but got [" + token + "]");
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(this.queryText);
        out.writeString(this.instance);

        if (this.intentFields != null) {
            out.writeInt(this.intentFields.size());
            for (String intentField : this.intentFields) {
                out.writeString(intentField);
            }
        }
    }

    protected boolean checkEquals(BaseHumaneQueryBuilder other) {
        return Objects.equals(queryText, other.queryText) &&
                Objects.equals(instance, other.instance) &&
                Objects.equals(intentFields, other.intentFields);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(this.queryText, this.instance, this.intentFields);
    }

    protected Query matchNoDocsQuery() {
        return Queries.newMatchNoDocsQuery(MATCHING_NO_DOCS_REASON);
    }

    protected Query parse(QueryShardContext queryShardContext, Client client, QueryField field) throws IOException {
        try {

            QueryField[] queryFields = {field};

            return humaneQuery(queryShardContext, client, queryFields);
        } catch (Throwable t) {
            logger.error("Error in creating humane query", t);
            throw t;
        }
    }

    protected Query parse(QueryShardContext queryShardContext, Client client, QueryField[] queryFields) throws IOException {
        try {
            return humaneQuery(queryShardContext, client, queryFields);
        } catch (Throwable t) {
            logger.error("Error in creating humane query", t);
            throw t;
        }
    }

    private NestedPathContext nestedPathContext(QueryShardContext queryShardContext, String indexName, String path) {
        String key = indexName + "/" + path;
        synchronized (NESTED_PATH_CONTEXT_CACHE) {
            if (!NESTED_PATH_CONTEXT_CACHE.containsKey(key)) {
                NestedPathContext nestedPathContext = new NestedPathContext();
                nestedPathContext.path = path;

                ObjectMapper nestedObjectMapper = queryShardContext.getObjectMapper(path);

                if (nestedObjectMapper == null) {
                    throw new ElasticsearchException("[nested] failed to find nested object under path [" + path + "]");
                }

                if (!nestedObjectMapper.nested().isNested()) {
                    throw new ElasticsearchException("[nested] nested object under path [" + path + "] is not of nested type");
                }

                ObjectMapper objectMapper = queryShardContext.nestedScope().getObjectMapper();
                if (objectMapper == null) {
                    nestedPathContext.parentFilter = queryShardContext.bitsetFilter(Queries.newNonNestedFilter());
                } else {
                    nestedPathContext.parentFilter = queryShardContext.bitsetFilter(objectMapper.nestedTypeFilter());
                }

                nestedPathContext.childFilter = nestedObjectMapper.nestedTypeFilter();

                // nestedPathContext.parentObjectMapper = parseContext.nestedScope().nextLevel(nestedPathContext.nestedObjectMapper);

                // todo: for multiple hierarchy do above in recursive manner
                // reset level
                // parseContext.nestedScope().previousLevel();

                NESTED_PATH_CONTEXT_CACHE.put(key, nestedPathContext);
            }

            return NESTED_PATH_CONTEXT_CACHE.get(key);
        }
    }

    private Analyzer analyzer(QueryShardContext queryShardContext, String analyzerName) {
        Analyzer analyzer = queryShardContext.getMapperService().analysisService().analyzer(analyzerName);
        if (analyzer == null) {
            throw new IllegalArgumentException("No analyzer found for [" + analyzerName + "]");
        }

        return analyzer;
    }

    private QueryBuilder queryBuilder(QueryShardContext queryShardContext, String analyzerName) {
        synchronized (QUERY_BUILDER_CACHE) {
            if (!QUERY_BUILDER_CACHE.containsKey(analyzerName)) {
                Analyzer analyzer = this.analyzer(queryShardContext, analyzerName);
                assert analyzer != null;

                QUERY_BUILDER_CACHE.put(analyzerName, new QueryBuilder(analyzer));
            }

            return QUERY_BUILDER_CACHE.get(analyzerName);
        }
    }

//    private Query multiFieldQuery(QueryShardContext queryShardContext, String indexName, QueryField[] queryFields, String text, Suggestion[] suggestions, boolean stopWord) {
//        if (queryFields.length == 1) {
//            return this.fieldQuery(queryShardContext, indexName, queryFields[0], text, suggestions, stopWord);
//        }
//
//        List<Query> fieldDisjuncts = new LinkedList<>();
//
//        for (QueryField queryField : queryFields) {
//            fieldDisjuncts.add(this.fieldQuery(queryShardContext, indexName, queryField, text, suggestions, stopWord));
//        }
//
//        return new DisjunctionMaxQuery(fieldDisjuncts, TIE_BREAKER_MULTIPLIER_DEFAULT);
//    }
//
//    // exact = 50 % field weight
//    // edgeGram = 30 % field weight
//    // phonetic = 15 % field weight
//    // edgeGramPhonetic = 5 % field weight
//    private Query fieldQuery(QueryShardContext queryShardContext, String indexName, QueryField field, String text, Suggestion[] suggestions, boolean stopWord) {
//        if (field.keyword) {
//            return buildQuery(queryShardContext, field, KEYWORD_ANALYZER_NAME, text, FieldType.Keyword, EXACT_MATCH_BOOST * field.boost);
//        }
//
//        BooleanQuery.Builder fieldQueryBuilder = new BooleanQuery.Builder();
//
//        if (!stopWord) {
//            fieldQueryBuilder.setMinimumNumberShouldMatch(1);
//        }
//
//        fieldQueryBuilder.add(buildQuery(queryShardContext, field, STANDARD_QUERY_ANALYZER_NAME, text, FieldType.Humane, EXACT_MATCH_BOOST * field.boost), BooleanClause.Occur.SHOULD);
//        fieldQueryBuilder.add(buildQuery(queryShardContext, field, STANDARD_QUERY_ANALYZER_NAME, text, FieldType.Shingle, EXACT_MATCH_BOOST * field.boost / 2), BooleanClause.Occur.SHOULD);
//        fieldQueryBuilder.add(buildQuery(queryShardContext, field, STANDARD_EDGE_GRAM_QUERY_ANALYZER_NAME, text, FieldType.Humane, EDGE_GRAM_MATCH_BOOST * field.boost), BooleanClause.Occur.SHOULD);
//        fieldQueryBuilder.add(buildQuery(queryShardContext, field, STANDARD_EDGE_GRAM_QUERY_ANALYZER_NAME, text, FieldType.Shingle, EDGE_GRAM_MATCH_BOOST * field.boost / 2), BooleanClause.Occur.SHOULD);
//
////        boolean addedShingleQueries = false;
//
//        boolean noFuzzy = field.noFuzzy || text.length() <= MIN_NO_FUZZY_TOKEN_LENGTH;
//
//        if (!noFuzzy && suggestions != null) {
//            for (Suggestion suggestion : suggestions) {
//                if (suggestion.isIgnore()
//                        || /*suggestion.getInputTokenCount() == EncodingType.Uni &&*/ (suggestion.getMatchStats().getMatchLevel() == MatchLevel.Exact || suggestion.getMatchStats().getMatchLevel() == MatchLevel.EdgeGram)) {
//                    continue;
//                }
//
////                boolean shingle = suggestion.getSuggestionTokenCount() > 1;
//
////                // only if there is at least one bi token type we add
////                if (!addedShingleQueries && shingle) {
////                    fieldQueryBuilder.add(buildQuery(queryShardContext, field, STANDARD_QUERY_ANALYZER_NAME, text, true,
////                            (suggestion.getSuggestionTokenCount() == EncodingType.Bi ? SHINGLE_EXACT_MATCH_BOOST : EXACT_MATCH_BOOST) * field.boost), BooleanClause.Occur.SHOULD);
////
////                    addedShingleQueries = true;
////                }
//
//                float boostMultiplier = DEFAULT_BOOST;
//                if (suggestion.getMatchStats().getMatchLevel() == MatchLevel.Synonym) {
//                    boostMultiplier = SYNONYM_MATCH_BOOST;
//                } /*else if (suggestion.getMatchStats().getMatchLevel() == MatchLevel.Exact) {
//                    boostMultiplier = SHINGLE_EXACT_MATCH_BOOST;
//                } else if (suggestion.getMatchStats().getMatchLevel() == MatchLevel.EdgeGram) {
//                    boostMultiplier = SHINGLE_EDGE_GRAM_BOOST;
//                }*/ else if (suggestion.getMatchStats().getMatchLevel() == MatchLevel.Phonetic) {
//                    boostMultiplier = PHONETIC_BOOST;
//                } else if (suggestion.getMatchStats().getMatchLevel() == MatchLevel.EdgeGramPhonetic) {
//                    boostMultiplier = DEFAULT_BOOST;
//                }
//
//                float weight = (boostMultiplier * field.boost * suggestion.getMatchStats().getScore()); // / (float) Math.pow(EDIT_DISTANCE_NORMALIZE_FACTOR, suggestion.getMatchStats().getEditDistance());
//                if (suggestion.getMatchStats().getScore() > 0) {
//                    weight *= suggestion.getMatchStats().getScore();
//                }
//
//                fieldQueryBuilder.add(buildQuery(queryShardContext, field, STANDARD_QUERY_ANALYZER_NAME, null /*TODO: suggestion.getSuggestionTokens()*/, FieldType.Humane, weight), BooleanClause.Occur.SHOULD);
//                fieldQueryBuilder.add(buildQuery(queryShardContext, field, STANDARD_QUERY_ANALYZER_NAME, null /*TODO: suggestion.getSuggestionTokens()*/, FieldType.Shingle, weight), BooleanClause.Occur.SHOULD);
//            }
//        }
//
//        Query query = fieldQueryBuilder.build();
//
//        // create nested path
//        if (field.path != null) {
//            NestedPathContext nestedPathContext = nestedPathContext(queryShardContext, indexName, field.path);
//            return new ToParentBlockJoinQuery(Queries.filtered(query, nestedPathContext.childFilter), nestedPathContext.parentFilter, ScoreMode.Avg);
//        }
//
//        return query;
//    }

    private Query buildQuery(QueryShardContext queryShardContext, QueryField field, String analyzer, String text, FieldType fieldType, float weight) {
        QueryBuilder queryBuilder = this.queryBuilder(queryShardContext, analyzer);

        String fieldName = field.name;

        if (fieldType != FieldType.Keyword) {
            fieldName += (fieldType == FieldType.Shingle ? SHINGLE_FIELD_SUFFIX : HUMANE_FIELD_SUFFIX);
        }

        Query query = queryBuilder.createBooleanQuery(fieldName, text);
        if (query instanceof TermQuery) {
            query = constantScoreQuery(query, weight);
        } else if (query instanceof BooleanQuery) {
            BooleanQuery bq = (BooleanQuery) query;
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            for (BooleanClause clause : bq.clauses()) {
                if (clause.getQuery() instanceof TermQuery) {
                    TermQuery termQuery = (TermQuery) clause.getQuery();
                    builder.add(constantScoreQuery(termQuery, weight), BooleanClause.Occur.SHOULD);
                } else {
                    builder.add(clause.getQuery(), BooleanClause.Occur.SHOULD);
                }
            }

            query = builder.build();
        }

        return query;
    }

    private Query constantScoreQuery(Query query, float weight) {
        if (weight == DEFAULT_BOOST) {
            return new ConstantScoreQuery(query);
        } else {
            return new BoostQuery(new ConstantScoreQuery(query), weight);
        }
    }

    private Query query(Query[] queryNodes, int numTokens) {
        int numQueryNodes = 0;
        Query lastNotNullQueryNode = null;
        for (int j = 0; j < numTokens; j++) {
            if (queryNodes[j] != null) {
                lastNotNullQueryNode = queryNodes[j];
                numQueryNodes++;
            }
        }

        if (numQueryNodes <= 1) {
            return lastNotNullQueryNode;
        }

        BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder();
        for (int j = 0; j < numTokens; j++) {
            if (queryNodes[j] != null) {
                booleanQueryBuilder.add(queryNodes[j], BooleanClause.Occur.SHOULD);
            }
        }

        booleanQueryBuilder.setMinimumNumberShouldMatch(minimumShouldMatch(numTokens));

        return booleanQueryBuilder.build();
    }

    private int minimumShouldMatch(int numTokens) {
        if (numTokens <= 2) {
            return 1;
        } else if (numTokens > 2 && numTokens <= 4) {
            return 2; //(int) Math.floor(0.90 * numTokens);
        } else { //if (numTokens > 4) {
            return 3; //(int) Math.floor(0.80 * numTokens);
        } /*else {
            return (int) Math.floor(0.70 * numTokens);
        }*/
    }

    @SuppressWarnings("unchecked")
    private Query humaneQuery(QueryShardContext queryShardContext, Client client, QueryField[] queryFields) throws IOException {

        long startTime = 0;

        if (logger.isDebugEnabled()) {
            startTime = System.currentTimeMillis();
        }

        String indexName = queryShardContext.index().getName();

        if (indexName == null) {
            indexName = "__all__";
        }

        Collection<String> queryTypesList = queryShardContext.queryTypes();
        String[] queryTypes = null;
        if (queryTypesList != null) {
            queryTypes = queryTypesList.toArray(new String[queryTypesList.size()]);
        }

        return null;

//        List<String> tokens = TOKENS_BUILDER.tokens(queryShardContext.getAnalysisService(), queryText);
//
//        if (logger.isDebugEnabled()) {
//            logger.debug("For Index/Type: {}/{} and queryText: {}, got Tokens: {} in {}ms", indexName, queryTypes, queryText, tokens, (System.currentTimeMillis() - startTime));
//        }
//
//        int numTokens = tokens.size();
//
//        if (numTokens == 0 || numTokens >= 8) {
//            return null;
//        }
//
//        if (indexName.containsInput(SEARCH_QUERY_STORE_SUFFIX)) {
//            Query[] queryNodes = new Query[numTokens];
//
//            for (int i = 0; i < numTokens; i++) {
//                String token = tokens.get(i);
//                queryNodes[i] = this.multiFieldQuery(queryShardContext, indexName, queryFields, token, null, false);
//            }
//
//            return query(queryNodes, numTokens);
//        } else {
//            if (logger.isDebugEnabled()) {
//                startTime = System.currentTimeMillis();
//            }
//
//            Map<String, Conjunct> conjunctMap = new HashMap<>();
//            Disjunct[] disjuncts = DISJUNCTS_BUILDER.build(tokens, conjunctMap, /*numTokens < 6 ? 3 : 1*/1);
//
//            if (logger.isDebugEnabled()) {
//                logger.debug("For Index/Type: {}/{} and tokens: {}, got disjuncts: {} in {}ms", indexName, queryTypes, tokens, Arrays.toString(disjuncts), (System.currentTimeMillis() - startTime));
//
//                startTime = System.currentTimeMillis();
//            }
//
//            Map<String, QueryTag> suggestionsMap = EmptySuggestionMap;
//            if (numTokens < 6) {
//                Set<DefaultTagWeight> suggestionScopes;
//                final String tokenStoreIndex = instance + TokenIndexConstants.TOKEN_STORE_SUFFIX;
//
//                if (intentFields == null || intentFields.size() == 0) {
//                    suggestionScopes = new HashSet<>();
//                } else {
//                    suggestionScopes = intentFields.stream().map(DefaultTagWeight::new).collect(Collectors.toSet());
//                }
//
//                suggestionsMap = TAG_BUILDER.fetchSuggestions(client,
//                        tokens.toArray(new String[tokens.size()]), // TODO: conjunctMap.values(),
//                        new String[]{tokenStoreIndex},
//                        suggestionScopes);
//            }
//
//            if (logger.isDebugEnabled()) {
//                logger.debug("For Index/Type: {}/{}, query: {}, tokens: {}, disjuncts: {}, got suggestions: {} in {}ms", indexName, queryTypes, queryText, tokens, Arrays.toString(disjuncts), suggestionsMap, (System.currentTimeMillis() - startTime));
//            }
//
//            List<Query> queries = new ArrayList<>();
//
//            for (Disjunct disjunct : disjuncts) {
//                if (logger.isDebugEnabled()) {
//                    logger.debug("Building query for disjunct: {}", disjunct.getKey());
//                }
//
//                int shouldClauseCount = 0;
//                int clauseCount = 0;
//                int stopWordsCount = 0;
//                boolean ignoreDisjunct = false;
//                List<Query> conjunctQueries = new ArrayList<>();
//                for (Conjunct conjunct : disjunct.getConjuncts()) {
//                    if (logger.isDebugEnabled()) {
//                        logger.debug("Building query for conjunct: {}", conjunct.getKey());
//                    }
//
//                    if (conjunct.getLength() == 1) {
//                        // we form normal query
//                        String token = null; // TODO: conjunct.getTokens().get(0);
//                        String key = conjunct.getKey();
//                        QueryTag queryTag = suggestionsMap == null ? null : suggestionsMap.get(key);
//
//                        if (queryTag != null && (queryTag.getSuggestions() != null || queryTag.isNumber() || queryTag.isStopWord())) {
//                            Query termQuery = this.multiFieldQuery(queryShardContext, indexName, queryFields, token, /*false, numTokens,*/ queryTag.getSuggestions(), queryTag.isStopWord());
//
//                            if (logger.isDebugEnabled()) {
//                                logger.debug("Building term query for token: {}, conjunct: {}, key: {}, suggestions: {}, query: {}", token, conjunct.getKey(), key, queryTag, termQuery);
//                            }
//
//                            if (queryTag.isStopWord()) {
//                                stopWordsCount++;
//                            }
//
//                            conjunctQueries.add(termQuery);
//
//                            clauseCount++;
//                            shouldClauseCount++;
//                        } else {
//                            Query termQuery = this.multiFieldQuery(queryShardContext, indexName, queryFields, token, null, false);
//
//                            conjunctQueries.add(termQuery);
//                            clauseCount++;
//                            shouldClauseCount++;
//                        }
//                    } else {
//                        // we form a shingle query
//                        String compoundToken = StringUtils.replace(conjunct.getTokens(), "");
//                        String compoundKey = conjunct.getKey();
//                        QueryTag queryTag = suggestionsMap == null ? null : suggestionsMap.get(compoundKey);
//
//                        if (queryTag != null && queryTag.getSuggestions() != null) {
//                            Query compoundQuery = this.multiFieldQuery(queryShardContext, indexName, queryFields, compoundToken, /*true, numTokens,*/ queryTag.getSuggestions(), queryTag.isStopWord());
//
//                            if (logger.isDebugEnabled()) {
//                                logger.debug("Building compound query for token: {}, conjunct: {}, key: {}, suggestions: {}, query: {}",
//                                        compoundToken,
//                                        conjunct.getKey(),
//                                        compoundKey,
//                                        queryTag,
//                                        compoundQuery);
//                            }
//
//                            if (queryTag.isStopWord()) {
//                                stopWordsCount++;
//                            }
//
//                            conjunctQueries.add(compoundQuery);
//
//                            clauseCount++;
//                            shouldClauseCount++;
//                        } else {
//                            ignoreDisjunct = true;
//                        }
//                    }
//                }
//
//                if (ignoreDisjunct || clauseCount == 0) {
//                    continue;
//                }
//
//                if (conjunctQueries.size() == 1) {
//                    queries.add(conjunctQueries.get(0));
//                } else {
//                    SmartMinMatchQuery.Builder disjunctQueryBuilder = new SmartMinMatchQuery.Builder();
////                    BooleanQuery.Builder disjunctQueryBuilder = new BooleanQuery.Builder();
//                    for (Query conjunctQuery : conjunctQueries) {
////                        disjunctQueryBuilder.add(conjunctQuery, BooleanClause.Occur.SHOULD);
//                        disjunctQueryBuilder.add(conjunctQuery);
//                    }
//
////                    // when we can pick field level weight from suggestion, then we can be okay with minimum match count as 1
////                    // that should be okay for search results, but may not be for autocomplete
////                    if (shouldClauseCount > 0) {
////                        int minimumNumberShouldMatch = minimumShouldMatch(Math.min(shouldClauseCount - stopWordsCount, shouldClauseCount));
////
////                        if (logger.isDebugEnabled()) {
////                            logger.debug("Setting {} should clauses setting min required count:  should: {}, min: {}, stop: {}", shouldClauseCount, minimumNumberShouldMatch, stopWordsCount);
////                        }
////
////                        disjunctQueryBuilder.setMinimumNumberShouldMatch(minimumNumberShouldMatch);
////
////                    }
//
//                    queries.add(disjunctQueryBuilder.build());
//                }
//            }
//
//            if (queries.size() == 0) {
//                return null;
//            } else if (queries.size() == 1) {
//                return queries.get(0);
//            }
//
//            return new DisjunctionMaxQuery(queries, TIE_BREAKER_MULTIPLIER_DEFAULT);
//        }
    }

    private static class NestedPathContext {
        String path;

        // TODO: maintain for deeper nesting
        // ObjectMapper nestedObjectMapper;
        // ObjectMapper parentObjectMapper;
        BitSetProducer parentFilter;
        Query childFilter;
    }

    protected static class QueryField implements Writeable {
        String name;
        String path;
        float boost = DEFAULT_BOOST;
        boolean keyword = false;
        boolean noFuzzy = false;
        boolean vernacularOnly = false;

        QueryField() {
        }

        QueryField(StreamInput in) throws IOException {
            this.name = in.readString();
            this.boost = in.readFloat();
            this.keyword = in.readBoolean();
            this.noFuzzy = in.readBoolean();
            this.vernacularOnly = in.readBoolean();
            this.path = in.readOptionalString();
        }

        @Override
        public String toString() {
            return "QueryField{" +
                    "name='" + name + '\'' +
                    ", path='" + path + '\'' +
                    ", boost=" + boost +
                    ", keyword=" + keyword +
                    ", noFuzzy=" + noFuzzy +
                    ", vernacularOnly=" + vernacularOnly +
                    '}';
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeFloat(boost);
            out.writeBoolean(keyword);
            out.writeBoolean(noFuzzy);
            out.writeBoolean(vernacularOnly);
            out.writeOptionalString(path);
        }
    }

    static enum FieldType {
        Shingle,
        Humane,
        Keyword
    }
}

package io.threesixtyfy.humaneDiscovery.query;

import io.threesixtyfy.humaneDiscovery.didYouMean.commons.Conjunct;
import io.threesixtyfy.humaneDiscovery.didYouMean.commons.Disjunct;
import io.threesixtyfy.humaneDiscovery.didYouMean.commons.DisjunctsBuilder;
import io.threesixtyfy.humaneDiscovery.didYouMean.commons.MatchLevel;
import io.threesixtyfy.humaneDiscovery.didYouMean.commons.Suggestion;
import io.threesixtyfy.humaneDiscovery.didYouMean.commons.SuggestionSet;
import io.threesixtyfy.humaneDiscovery.didYouMean.commons.SuggestionsBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.search.join.ToParentBlockJoinQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.QueryBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryParsingException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class HumaneQuery extends Query {
    private final ESLogger logger = Loggers.getLogger(HumaneQuery.class);

    private static final String StandardQueryAnalyzerName = "humane_query_analyzer";
    private static final String StandardEdgeGramQueryAnalyzerName = "humane_edgeGram_query_analyzer";

    private static final Map<String, QueryBuilder> QueryBuilderCache = new HashMap<>();

    private static final Map<String, NestedPathContext> NestedPathContextCache = new HashMap<>();

    protected final QueryParseContext parseContext;

    private final DisjunctsBuilder disjunctsBuilder = new DisjunctsBuilder();

    public HumaneQuery(QueryParseContext parseContext) {
        this.parseContext = parseContext;
    }

    public Query parse(Client client, SuggestionsBuilder suggestionsBuilder, QueryField field, Object value) throws IOException {
        try {
            QueryField[] queryFields = {field};

            return humaneQuery(client, suggestionsBuilder, queryFields, value.toString());
        } catch (Throwable t) {
            logger.error("Error in creating humane query", t);
            throw t;
        }
    }

    public Query parse(Client client, SuggestionsBuilder suggestionsBuilder, QueryField[] fields, Object value) throws IOException {
        try {
            return humaneQuery(client, suggestionsBuilder, fields, value.toString());
        } catch (Throwable t) {
            logger.error("Error in creating humane query", t);
            throw t;
        }
    }

    protected NestedPathContext nestedPathContext(String path) {
        synchronized (NestedPathContextCache) {
            if (!NestedPathContextCache.containsKey(path)) {
                NestedPathContext nestedPathContext = new NestedPathContext();
                nestedPathContext.path = path;

                ObjectMapper nestedObjectMapper = parseContext.getObjectMapper(path);

                if (nestedObjectMapper == null) {
                    throw new QueryParsingException(parseContext, "[nested] failed to find nested object under path [" + path + "]");
                }

                if (!nestedObjectMapper.nested().isNested()) {
                    throw new QueryParsingException(parseContext, "[nested] nested object under path [" + path + "] is not of nested type");
                }

                ObjectMapper objectMapper = parseContext.nestedScope().getObjectMapper();
                if (objectMapper == null) {
                    nestedPathContext.parentFilter = parseContext.bitsetFilter(Queries.newNonNestedFilter());
                } else {
                    nestedPathContext.parentFilter = parseContext.bitsetFilter(objectMapper.nestedTypeFilter());
                }

                nestedPathContext.childFilter = nestedObjectMapper.nestedTypeFilter();

                // nestedPathContext.parentObjectMapper = parseContext.nestedScope().nextLevel(nestedPathContext.nestedObjectMapper);

                // todo: for multiple hierarchy do above in recursive manner
                // reset level
                // parseContext.nestedScope().previousLevel();

                NestedPathContextCache.put(path, nestedPathContext);
            }

            return NestedPathContextCache.get(path);
        }
    }

    protected Analyzer analyzer(String analyzerName) {
        Analyzer analyzer = parseContext.mapperService().analysisService().analyzer(analyzerName);
        if (analyzer == null) {
            throw new IllegalArgumentException("No analyzer found for [" + analyzerName + "]");
        }

        return analyzer;
    }

    protected QueryBuilder queryBuilder(String analyzerName) {
        synchronized (QueryBuilderCache) {
            if (!QueryBuilderCache.containsKey(analyzerName)) {
                Analyzer analyzer = this.analyzer(analyzerName);
                assert analyzer != null;

                QueryBuilderCache.put(analyzerName, new QueryBuilder(analyzer));
            }

            return QueryBuilderCache.get(analyzerName);
        }
    }

    protected Query multiFieldQuery(QueryField[] queryFields, String text, /*boolean shingle, int numTokens,*/ Suggestion[] suggestions) {
        if (queryFields.length == 1) {
            return this.fieldQuery(queryFields[0], text, /*shingle, numTokens,*/ suggestions);
        }

        List<Query> fieldDisjuncts = new LinkedList<>();

        for (QueryField queryField : queryFields) {
            fieldDisjuncts.add(this.fieldQuery(queryField, text, /*shingle, numTokens,*/ suggestions));
        }

        return new DisjunctionMaxQuery(fieldDisjuncts, 1.0f);
    }

    // exact = 50 % field weight
    // edgeGram = 30 % field weight
    // phonetic = 15 % field weight
    // edgeGramPhonetic = 5 % field weight
    protected Query fieldQuery(QueryField field, String text, /*boolean shingle, int numTokens,*/ Suggestion[] suggestions) {
        BooleanQuery.Builder fieldQueryBuilder = new BooleanQuery.Builder();

        boolean noFuzzy = field.noFuzzy || text.length() <= 2;

        fieldQueryBuilder.add(buildQuery(field, StandardQueryAnalyzerName, text, false, 100.0f * field.boost), BooleanClause.Occur.SHOULD);
        fieldQueryBuilder.add(buildQuery(field, StandardEdgeGramQueryAnalyzerName, text, false, 20.0f * field.boost), BooleanClause.Occur.SHOULD);

        boolean addedShingleQueries = false;

        if (!noFuzzy && suggestions != null) {
            for (Suggestion suggestion : suggestions) {
                if (suggestion.isIgnore()
                        || suggestion.getTokenType() == Suggestion.TokenType.Uni && (suggestion.getMatchLevel() == MatchLevel.Exact || suggestion.getMatchLevel() == MatchLevel.EdgeGram)) {
                    continue;
                }

                // only if there is at least one bi token type we add
                if (!addedShingleQueries && suggestion.getTokenType() == Suggestion.TokenType.Bi) {
                    fieldQueryBuilder.add(buildQuery(field, StandardQueryAnalyzerName, text, true, 200.0f * field.boost), BooleanClause.Occur.SHOULD);
                    // fieldQueryBuilder.add(buildQuery(field, StandardEdgeGramQueryAnalyzerName, text, true, 20.0f * field.boost), BooleanClause.Occur.SHOULD);

                    addedShingleQueries = true;
                }

                float boostMultiplier = 1.0f;
                if (suggestion.getMatchLevel() == MatchLevel.Exact) {
                    boostMultiplier = 200.0f;
                } else if (suggestion.getMatchLevel() == MatchLevel.EdgeGram) {
                    boostMultiplier = 40.0f;
                } else if (suggestion.getMatchLevel() == MatchLevel.Phonetic) {
                    if (suggestion.getTokenType() == Suggestion.TokenType.Bi) {
                        boostMultiplier = 10.0f;
                    } else {
                        boostMultiplier = 5.0f;
                    }
                } else if (suggestion.getMatchLevel() == MatchLevel.EdgeGramPhonetic) {
                    if (suggestion.getTokenType() == Suggestion.TokenType.Bi) {
                        boostMultiplier = 2.0f;
                    } else {
                        boostMultiplier = 1.0f;
                    }
                }

                fieldQueryBuilder.add(
                        buildQuery(field,
                                StandardQueryAnalyzerName,
                                suggestion.getSuggestion(),
                                suggestion.getTokenType() == Suggestion.TokenType.Bi,
                                (1 - suggestion.getEditDistance() / 10.0f) * boostMultiplier * field.boost),
                        BooleanClause.Occur.SHOULD);
            }
        }

        Query query = fieldQueryBuilder.build();

        // create nested path
        if (field.path != null) {
            NestedPathContext nestedPathContext = nestedPathContext(field.path);
            return new ToParentBlockJoinQuery(Queries.filtered(query, nestedPathContext.childFilter), nestedPathContext.parentFilter, ScoreMode.Avg);
        }

        return query;
    }

    protected Query buildQuery(QueryField field, String analyzer, String text, boolean shingle, float weight) {
        QueryBuilder queryBuilder = this.queryBuilder(analyzer);

        String fieldName = field.name + (shingle ? ".shingle" : ".humane");

        Query query = queryBuilder.createBooleanQuery(fieldName, text);
        if (query instanceof TermQuery) {
            query = constantScoreQuery(query, shingle ? 10.0f * weight : weight);
        } else if (query instanceof BooleanQuery) {
            BooleanQuery bq = (BooleanQuery) query;
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            for (BooleanClause clause : bq.clauses()) {
                if (clause.getQuery() instanceof TermQuery) {
                    TermQuery termQuery = (TermQuery) clause.getQuery();
                    builder.add(constantScoreQuery(termQuery, shingle ? 10.0f * weight : weight), BooleanClause.Occur.SHOULD);
                } else {
                    builder.add(clause.getQuery(), BooleanClause.Occur.SHOULD);
                }
            }

            query = builder.build();
        }

        return query;
    }

    protected Query constantScoreQuery(Query query, float weight) {
        if (weight == 1.0f) {
            return new ConstantScoreQuery(query);
        } else {
            return new BoostQuery(new ConstantScoreQuery(query), weight);
        }
    }

    public static String toString(BytesRef termText) {
        // the term might not be text, but usually is. so we make a best effort
        CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
        try {
            return decoder.decode(ByteBuffer.wrap(termText.bytes, termText.offset, termText.length)).toString();
        } catch (CharacterCodingException e) {
            return termText.toString();
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
            return numTokens;
        } else if (numTokens <= 4) {
            return (int) Math.floor(0.90 * numTokens);
        } else if (numTokens <= 6) {
            return (int) Math.floor(0.80 * numTokens);
        } else {
            return (int) Math.floor(0.70 * numTokens);
        }
    }

    @SuppressWarnings("unchecked")
    protected Query humaneQuery(Client client, SuggestionsBuilder suggestionsBuilder, QueryField[] queryFields, String queryText) throws IOException {

        long startTime = 0;

        if (logger.isDebugEnabled()) {
            startTime = System.currentTimeMillis();
        }

        String indexName = this.parseContext.index().name();

        Collection<String> queryTypesList = this.parseContext.queryTypes();
        String[] queryTypes = null;
        if (queryTypesList != null) {
            queryTypes = queryTypesList.toArray(new String[queryTypesList.size()]);
        }

        List<String> tokens = suggestionsBuilder.tokens(this.parseContext.analysisService(), queryText);

        if (logger.isDebugEnabled()) {
            logger.debug("For Index/Type: {}/{} and queryText: {}, got Tokens: {} in {}ms", indexName, queryTypes, queryText, tokens, (System.currentTimeMillis() - startTime));
        }

        int numTokens = tokens.size();

        if (numTokens == 0) {
            return null;
        }

        if (indexName.contains(":search_query_store")) {
            Query[] queryNodes = new Query[numTokens];

            for (int i = 0; i < numTokens; i++) {
                String token = tokens.get(i);
                queryNodes[i] = this.multiFieldQuery(queryFields, token, /*false, numTokens,*/ null);
            }

            return query(queryNodes, numTokens);
        } else {
            if (logger.isDebugEnabled()) {
                startTime = System.currentTimeMillis();
            }

            Map<String, Conjunct> conjunctMap = new HashMap<>();
            Disjunct[] disjuncts = disjunctsBuilder.build(tokens, conjunctMap);

            if (logger.isDebugEnabled()) {
                logger.debug("For Index/Type: {}/{} and tokens: {}, got disjuncts: {} in {}ms", indexName, queryTypes, tokens, Arrays.toString(disjuncts), (System.currentTimeMillis() - startTime));

                startTime = System.currentTimeMillis();
            }

            final Map<String, SuggestionSet> suggestionsMap = suggestionsBuilder.fetchSuggestions(client, conjunctMap.values(), indexName + ":did_you_mean_store");

            if (logger.isDebugEnabled()) {
                logger.debug("For Index: {}/{}, query: {}, tokens: {}, disjuncts: {}, got suggestions: {} in {}ms", indexName, queryTypes, queryText, tokens, Arrays.toString(disjuncts), suggestionsMap, (System.currentTimeMillis() - startTime));
            }

            if (suggestionsMap == null || suggestionsMap.size() == 0) {
                return null;
            }

            List<Query> queries = new ArrayList<>();

            for (Disjunct disjunct : disjuncts) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Building query for disjunct: {}", disjunct.getKey());
                }

                int shouldClauseCount = 0;
                int clauseCount = 0;
                int stopWordsCount = 0;
                boolean ignoreDisjunct = false;
                BooleanQuery.Builder disjunctQueryBuilder = new BooleanQuery.Builder();
                for (Conjunct conjunct : disjunct.getConjuncts()) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Building query for conjunct: {}", conjunct.getKey());
                    }

                    if (conjunct.getLength() == 1) {
                        // we form normal query
                        String token = conjunct.getTokens().get(0);
                        String key = conjunct.getKey();
                        SuggestionSet suggestionSet = suggestionsMap.get(key);

                        if (suggestionSet != null && (suggestionSet.getSuggestions() != null || suggestionSet.isNumber() || suggestionSet.isStopWord())) {
                            Query termQuery = this.multiFieldQuery(queryFields, token, /*false, numTokens,*/ suggestionSet.getSuggestions());

                            if (logger.isDebugEnabled()) {
                                logger.debug("Building term query for token: {}, conjunct: {}, key: {}, suggestions: {}, query: {}", token, conjunct.getKey(), key, suggestionSet, termQuery);
                            }

//                            if (suggestionSet.isStopWord()) {
//                                stopWordsCount++;
//                            }

                            disjunctQueryBuilder.add(termQuery, BooleanClause.Occur.SHOULD);

                            clauseCount++;
                            shouldClauseCount++;

                        } /*else if (queryTypes != null && queryTypes.length == 1 && (queryTypes[0].contains("new_car_model") || queryTypes[0].contains("new_car_brand"))) {
                            // TODO: either support this basis some flag... or some other way...
                            ignoreDisjunct = true;
                            break;
                        }*/
                    } else {
                        // we form a shingle query
                        String compoundToken = StringUtils.join(conjunct.getTokens(), "");
                        String compoundKey = conjunct.getKey(); //+ "/shingle";
                        SuggestionSet suggestionSet = suggestionsMap.get(compoundKey);

                        if (suggestionSet != null && suggestionSet.getSuggestions() != null) {
                            Query compoundQuery = this.multiFieldQuery(queryFields, compoundToken, /*true, numTokens,*/ suggestionSet.getSuggestions());

                            if (logger.isDebugEnabled()) {
                                logger.debug("Building compound query for token: {}, conjunct: {}, key: {}, suggestions: {}, query: {}",
                                        compoundToken,
                                        conjunct.getKey(),
                                        compoundKey,
                                        suggestionSet,
                                        compoundQuery);
                            }

                            disjunctQueryBuilder.add(compoundQuery, BooleanClause.Occur.SHOULD);

                            clauseCount++;
                            shouldClauseCount++;
                        } else {
                            ignoreDisjunct = true;
                        }
                    }
                }

                if (ignoreDisjunct) {
                    continue;
                }

                // when we can pick field level weight from suggestion, then we can be okay with minimum match count as 1
                // that should be okay for search results, but may not be for autocomplete
                if (shouldClauseCount > 0) {
                    int minimumNumberShouldMatch;
                    // TODO: another way is let all the results come in, as normal... but determine the level based on tokens in the query
                    if (queryTypes != null && queryTypes.length == 1 && (queryTypes[0].contains("new_car_model") || queryTypes[0].contains("new_car_brand"))) {
                        // all tokens are required
                        minimumNumberShouldMatch = shouldClauseCount;
                    } else {
                        minimumNumberShouldMatch = minimumShouldMatch(shouldClauseCount);
                    }

                    minimumNumberShouldMatch = Math.min(shouldClauseCount - stopWordsCount, minimumNumberShouldMatch);

                    if (logger.isDebugEnabled()) {
                        logger.debug("Setting {} should clauses setting min required count:  should: {}, min: {}, stop: {}", shouldClauseCount, minimumNumberShouldMatch, stopWordsCount);
                    }

                    disjunctQueryBuilder.setMinimumNumberShouldMatch(minimumNumberShouldMatch);
                }

                if (clauseCount > 0) {
                    queries.add(disjunctQueryBuilder.build());
                }
            }

            if (queries.size() == 0) {
                return null;
            } else if (queries.size() == 1) {
                return queries.get(0);
            }

            return new DisjunctionMaxQuery(queries, 1.0f);
        }
    }

    @Override
    public String toString(String field) {
        return getClass().getSimpleName();
    }

    static class NestedPathContext {
        String path;
        // ObjectMapper nestedObjectMapper;
        // ObjectMapper parentObjectMapper;
        BitSetProducer parentFilter;
        Query childFilter;
    }

}

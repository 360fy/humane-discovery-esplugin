package io.threesixtyfy.humaneDiscovery.core.spellSuggestion;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.threesixtyfy.humaneDiscovery.core.conjuncts.Conjunct;
import io.threesixtyfy.humaneDiscovery.core.dictionary.StandardSynonyms;
import io.threesixtyfy.humaneDiscovery.core.dictionary.StopWords;
import io.threesixtyfy.humaneDiscovery.core.encoding.Constants;
import io.threesixtyfy.humaneDiscovery.core.encoding.EncodingsBuilder;
import io.threesixtyfy.humaneDiscovery.core.utils.EditDistanceUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.threesixtyfy.humaneDiscovery.core.encoding.Constants.BIGRAM_DID_YOU_MEAN_INDEX_TYPE;
import static io.threesixtyfy.humaneDiscovery.core.encoding.Constants.GRAM_END_PREFIX_LENGTH;
import static io.threesixtyfy.humaneDiscovery.core.encoding.Constants.GRAM_PREFIX;
import static io.threesixtyfy.humaneDiscovery.core.encoding.Constants.GRAM_PREFIX_LENGTH;
import static io.threesixtyfy.humaneDiscovery.core.encoding.Constants.GRAM_START_PREFIX_LENGTH;
import static io.threesixtyfy.humaneDiscovery.core.encoding.Constants.UNIGRAM_DID_YOU_MEAN_INDEX_TYPE;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;

public class SpellSuggestionsBuilder {

    public static final float GRAM_START_BOOST = 10.0f;
    public static final float GRAM_END_BOOST = 10.0f;
    public static final float EXACT_TERM_BOOST = 100.0f;
    public static final int MINIMUM_NUMBER_SHOULD_MATCH = 2;
    public static final String FIELD_TOTAL_WEIGHT = "totalWeight";
    public static final String FIELD_TOTAL_COUNT = "totalCount";
    public static final String FIELD_COUNT_AS_FULL_WORD = "countAsFullWord";
    public static final String FIELD_ENCODINGS = "encodings";
    public static final String FIELD_ORIGINAL_WORDS = "originalWords";
    public static final String FIELD_WORD = "word";
    public static final String FIELD_DISPLAY = "display";
    public static final String FIELD_WORD_1 = "word1";
    public static final String FIELD_WORD_2 = "word2";
    public static final String FIELD_STATS = "fieldStats";
    public static final String[] FETCH_SOURCES = new String[]{
            FIELD_ENCODINGS,
            FIELD_TOTAL_WEIGHT,
            FIELD_TOTAL_COUNT,
            FIELD_COUNT_AS_FULL_WORD,
            FIELD_WORD,
            FIELD_WORD_1,
            FIELD_WORD_2,
            FIELD_STATS,
            "originalWords.*"};
    public static final String FIELD_WORD_1_ENCODINGS = "word1Encodings";
    public static final String FIELD_WORD_2_ENCODINGS = "word2Encodings";

    public static final String FIELD_FIELD_NAME = "fieldName";

    private final ESLogger logger = Loggers.getLogger(SpellSuggestionsBuilder.class);

    private final SuggestionSet NumberSuggestion = new SuggestionSet(true, false, null);

    private final SuggestionSet SingleLetterSuggestion = new SuggestionSet(false, false, new Suggestion[0]);

    private StandardSynonyms standardSynonyms = new StandardSynonyms();

    private final EncodingsBuilder encodingsBuilder = new EncodingsBuilder();

    // TODO: expire on event only
    private final Cache<String, SuggestionSet> CachedSuggestionSets = CacheBuilder
            .newBuilder()
            .maximumSize(1000)
            .expireAfterAccess(30, TimeUnit.SECONDS)
            .build();

//    private final ExecutorService futureExecutorService = new ThreadPoolExecutor(32, 32, 10, TimeUnit.SECONDS, new LinkedBlockingQueue<>(256));

    public SpellSuggestionsBuilder() {
//        NumberSuggestion.complete(new SuggestionSet(true, false, null));

//        SingleLetterSuggestion.complete(new SuggestionSet(false, false, new Suggestion[0]));
    }

    // if there are other suggestions, then ignore the one with very low count
    // else consider them ==> rejection shall happen at a very late stage
    // and selection is conditional to what other suggestions exist and what's their strength
    // e.g. hund for cardekho
    public Map<String, SuggestionSet> fetchSuggestions(Client client, Collection<Conjunct> conjuncts, String[] indices, Set<? extends SuggestionScope> suggestionScopes) {
        TaskContext taskContext = new TaskContext(client, indices, suggestionScopes);

        try {
            return fetchSuggestions(taskContext, conjuncts);
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Error in fetching suggestions for conjuncts: {}, indices: {}", e, conjuncts, indices);
        }

        return null;
    }

    private SuggestionSet store(TaskContext taskContext, String key, SuggestionSet future) {
        taskContext.queryKeys.add(key);
//        taskContext.suggestionResponses.add(suggestionSet);

        return future;
    }

    private SuggestionSet getOrBuildSuggestionSet(TaskContext taskContext, String key, String word, Function<Boolean, SuggestionSet> futureBuilder) throws ExecutionException {
        boolean number = NumberUtils.isNumber(word);

        // we do not store these in CachedSuggestionSets... as re-calculating them would not be that costly
        if (number) {
            return store(taskContext, key, NumberSuggestion);
        }

        boolean stopWord = StopWords.contains(word);

        if (word.length() == 1) {
            return store(taskContext, key, SingleLetterSuggestion);
        }

        return store(taskContext, key,
                CachedSuggestionSets.get(key(taskContext, key),
                        () -> futureBuilder.apply(stopWord)));
    }

    private boolean hasResults(SearchResponse searchResponse) {
        return !(searchResponse == null || searchResponse.getHits() == null || searchResponse.getHits().getHits() == null || searchResponse.getHits().getHits().length == 0);
    }

    @SuppressWarnings("unchecked")
    private SuggestionSet phoneticSuggestions(final TaskContext taskContext, String key, String word, TokenType inputTokenType, boolean stopWord, Supplier<QueryBuilderHolder> builder) {
        QueryBuilderHolder queryBuilderHolder = builder.get();

        QueryBuilder[] queryBuilders = queryBuilderHolder.queryBuilders;
        Set<String> encodings = queryBuilderHolder.encodings;

        List<SearchResponse> searchResponses = new ArrayList<>(queryBuilders.length);

        for (QueryBuilder queryBuilder : queryBuilders) {
            SearchResponse searchResponse = taskContext.client.prepareSearch(taskContext.indices)
                    .setSize(15)
                    .setQuery(queryBuilder)
                    .setFetchSource(FETCH_SOURCES, null)
                    .execute()
                    .actionGet(400, TimeUnit.MILLISECONDS);
            searchResponses.add(searchResponse);
        }

        List<SearchHit> searchHits = new ArrayList<>();

        for (SearchResponse searchResponse : searchResponses) {
            if (hasResults(searchResponse)) {
                Collections.addAll(searchHits, searchResponse.getHits().getHits());
            }
        }

        Set<Suggestion> suggestions = wordSuggestions(taskContext, word, inputTokenType, searchHits.toArray(new SearchHit[searchHits.size()]), encodings);

        return new SuggestionSet(false, stopWord, suggestions == null ? null : suggestions.toArray(new Suggestion[suggestions.size()]));
    }

    private SuggestionSet suggestionSet(final TaskContext taskContext, String key, String word, TokenType inputTokenType, boolean stopWord, Supplier<QueryBuilderHolder> queryBuilders) {
        if (logger.isDebugEnabled()) {
            logger.debug("Building suggestionSet for key: {}, word: {} indices: {}, query: {}", key, word, taskContext.indices);
        }

        SearchResponse searchResponse = taskContext.client.prepareSearch(taskContext.indices)
                .setSize(2)
                .setTypes(UNIGRAM_DID_YOU_MEAN_INDEX_TYPE, BIGRAM_DID_YOU_MEAN_INDEX_TYPE)
                .setQuery(buildWordQuery(taskContext, word))
                .setFetchSource(FETCH_SOURCES, null)
                .execute()
                .actionGet(200, TimeUnit.MILLISECONDS);

        if (!hasResults(searchResponse)) {
            // we build with phonetic way
            return phoneticSuggestions(taskContext, key, word, inputTokenType, stopWord, queryBuilders);
        }

        Set<Suggestion> suggestions = wordSuggestions(taskContext, word, inputTokenType, searchResponse.getHits().getHits(), null);

        return new SuggestionSet(false, stopWord, suggestions == null ? null : suggestions.toArray(new Suggestion[suggestions.size()]));
    }

    private QueryBuilder buildSuggestionScope(TaskContext taskContext) {
        BoolQueryBuilder scopeQueryBuilder = new BoolQueryBuilder();
        for (SuggestionScope suggestionScope : taskContext.suggestionScopes) {
            scopeQueryBuilder.should(QueryBuilders.nestedQuery("fieldStats", QueryBuilders.termQuery("fieldStats.fieldName", suggestionScope.getEntityName())));
        }

        scopeQueryBuilder.minimumNumberShouldMatch(1);

        return scopeQueryBuilder;
    }

    private SuggestionSet unigramWordMatch(TaskContext taskContext, Conjunct conjunct) throws ExecutionException {
        String key = conjunct.getKey();
        String word = conjunct.getTokens().get(0);

        return getOrBuildSuggestionSet(taskContext, key, word, (stopWord) -> suggestionSet(taskContext, key, word, TokenType.Uni, stopWord,
                () -> {
                    Set<String> encodings = encodingsBuilder.encodings(word, stopWord);

                    if (logger.isDebugEnabled()) {
                        logger.debug("Encoding for word {} = {}", word, encodings);
                    }

                    QueryBuilder[] queryBuilders = new QueryBuilder[]{
                            buildEncodingQuery(taskContext, Constants.UNIGRAM_DID_YOU_MEAN_INDEX_TYPE, FIELD_ENCODINGS, word, encodings),
                            buildEncodingQuery(taskContext, Constants.BIGRAM_DID_YOU_MEAN_INDEX_TYPE, FIELD_ENCODINGS, word, encodings)
                    };

                    return new QueryBuilderHolder(encodings, queryBuilders);
                }));
    }

    @SuppressWarnings("unchecked")
    private SuggestionSet compoundWordMatch(TaskContext taskContext, Conjunct conjunct) throws ExecutionException {
        String word = StringUtils.join(conjunct.getTokens(), "");
        String key = conjunct.getKey();

        return getOrBuildSuggestionSet(taskContext, key, word, (stopWord) -> suggestionSet(taskContext, key, word, TokenType.Bi, stopWord,
                () -> {
                    Set<String> encodings = encodingsBuilder.encodings(word, stopWord);

                    if (logger.isDebugEnabled()) {
                        logger.debug("Encoding for word {} = {}", word, encodings);
                    }

                    final QueryBuilder[] queryBuilders = new QueryBuilder[conjunct.getLength() == 2 ? 3 : 2];

                    queryBuilders[0] = buildEncodingQuery(taskContext, Constants.UNIGRAM_DID_YOU_MEAN_INDEX_TYPE, FIELD_ENCODINGS, word, encodings);
                    queryBuilders[1] = buildEncodingQuery(taskContext, Constants.BIGRAM_DID_YOU_MEAN_INDEX_TYPE, FIELD_ENCODINGS, word, encodings);

                    if (conjunct.getLength() == 2) {
                        // we can form bigram query for these
                        String word1 = conjunct.getTokens().get(0);
                        String word2 = conjunct.getTokens().get(1);

                        Set<String> word1Encodings = encodingsBuilder.encodings(word1, stopWord);
                        Set<String> word2Encodings = encodingsBuilder.encodings(word2, stopWord);

                        if (logger.isDebugEnabled()) {
                            logger.debug("Encoding for word 1 {} = {}", word1, word1Encodings);
                            logger.debug("Encoding for word 2 {} = {}", word2, word2Encodings);
                        }

                        QueryBuilder shingleQueryBuilder = boolQuery()
                                .must(buildEncodingQuery(taskContext, Constants.BIGRAM_DID_YOU_MEAN_INDEX_TYPE, FIELD_WORD_1_ENCODINGS, word1, word1Encodings))
                                .must(buildEncodingQuery(taskContext, Constants.BIGRAM_DID_YOU_MEAN_INDEX_TYPE, FIELD_WORD_2_ENCODINGS, word2, word2Encodings));

                        queryBuilders[2] = shingleQueryBuilder;
                    }

                    return new QueryBuilderHolder(encodings, queryBuilders);
                }));
    }

    private String key(TaskContext taskContext, String key) {
        return taskContext.id + "/" + key;
    }

    public Map<String, SuggestionSet> fetchSuggestions(TaskContext taskContext, Collection<Conjunct> conjuncts) throws ExecutionException, InterruptedException {
        if (conjuncts == null || conjuncts.size() == 0) {
            return null;
        }

        long startTime = System.currentTimeMillis();

        List<SuggestionSet> suggestionSets = new ArrayList<>(conjuncts.size());
        for (Conjunct conjunct : conjuncts) {
            if (conjunct.getLength() == 1) {
                suggestionSets.add(unigramWordMatch(taskContext, conjunct));
            } else {
                suggestionSets.add(compoundWordMatch(taskContext, conjunct));
            }
        }

        if (logger.isDebugEnabled()) {
            logger.debug("For conjuncts: {} and indices: {} build completable search responses: {} in {}ms", conjuncts, taskContext.indices, taskContext.suggestionResponses, (System.currentTimeMillis() - startTime));
        }

        startTime = System.currentTimeMillis();

        Map<String, SuggestionSet> suggestionsMap = new HashMap<>();

        int index = 0;
        for (SuggestionSet suggestions : suggestionSets) {
            suggestionsMap.put(taskContext.queryKeys.get(index), suggestions);
            index++;
        }

        if (logger.isDebugEnabled()) {
            logger.debug("For conjuncts: {} and indices: {} created suggestions: {} in {}ms", conjuncts, taskContext.indices, suggestionsMap, (System.currentTimeMillis() - startTime));
        }

        return suggestionsMap;
    }

    private BoolQueryBuilder buildEncodingQuery(TaskContext taskContext, String type, String field, String word, Set<String> phoneticEncodings) {
        BoolQueryBuilder boolQueryBuilder = boolQuery()
                .filter(QueryBuilders.typeQuery(type))
                .filter(buildSuggestionScope(taskContext))
                .should(QueryBuilders.termQuery(field, word).boost(EXACT_TERM_BOOST));

        phoneticEncodings.forEach(w -> {
            TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery(field, w);

            int length = w.length();

            if (w.startsWith(Constants.GRAM_START_PREFIX)) {
                termQueryBuilder.boost(GRAM_START_BOOST * (length - GRAM_START_PREFIX_LENGTH));
            } else if (w.startsWith(Constants.GRAM_END_PREFIX)) {
                termQueryBuilder.boost(GRAM_END_BOOST * (length - GRAM_END_PREFIX_LENGTH));
            } else if (w.startsWith(GRAM_PREFIX)) {
                termQueryBuilder.boost(length - GRAM_PREFIX_LENGTH);
            }

            boolQueryBuilder.should(termQueryBuilder);
        });

        int clauses = 1 + phoneticEncodings.size();
        if (clauses >= MINIMUM_NUMBER_SHOULD_MATCH) {
            boolQueryBuilder.minimumNumberShouldMatch(MINIMUM_NUMBER_SHOULD_MATCH);
        } else {
            boolQueryBuilder.minimumNumberShouldMatch(clauses);
        }

        return boolQueryBuilder;
    }

    private BoolQueryBuilder buildWordQuery(TaskContext taskContext, String word) {
        return boolQuery()
                .filter(buildSuggestionScope(taskContext))
                .filter(QueryBuilders.idsQuery().addIds(word));
    }

    private boolean highEditDistance(Suggestion.MatchStats currentStats, String inputWord, String suggestedWord) {
        boolean isHigh = currentStats.editDistance >= 5 /* || currentStats.lDistance < 0.5 || currentStats.jwDistance < 0.5*/;

        if (isHigh) {
            if (logger.isDebugEnabled()) {
                logger.debug("High edit distance for input: {}, suggested: {}, stats: {}", inputWord, suggestedWord, currentStats);
            }
        }

        return isHigh;
    }

    private boolean goodSuggestion(Suggestion.MatchStats bestStats, Suggestion.MatchStats currentStats, int inputWordLength) {
        if (bestStats != null) {
            if (inputWordLength <= 4 && currentStats.editDistance / inputWordLength > 0.40) {
                return false;
            }

            if (inputWordLength > 4 && currentStats.editDistance / inputWordLength > 0.50) {
                return false;
            }

            // we relax at the most one edit distance from the best match
            if (bestStats.editDistance != currentStats.editDistance) {
                return false;
            }

            // more than 5 times drop from best score -- earlier 3 times
            if (currentStats.score * 3.0 < bestStats.score) {
                return false;
            }
        }

        return true;
    }

    @SuppressWarnings("unchecked")
    private void buildSuggestion(TaskContext taskContext,
                                 Suggestion.MatchStats bestStats,
                                 Suggestion.MatchStats matchStats,
                                 SearchHit searchHit,
                                 String inputWord,
                                 TokenType inputTokenType,
                                 TokenType matchTokenType,
                                 String matchedWord,
                                 String display,
                                 int inputWordLength,
                                 Map<String, Suggestion> suggestionMap,
                                 Set<String> inputWordEncodings) {
//        double totalWeight = fieldValue(searchHit, FIELD_TOTAL_WEIGHT, 0.0d);
        int totalCount = fieldValue(searchHit, FIELD_TOTAL_COUNT, 0);
        List<Object> suggestedWordEncodings = fieldValues(searchHit, FIELD_ENCODINGS);

        // suggested word is prefix of input word
        if (inputTokenType == TokenType.Bi && matchTokenType == TokenType.Bi && !inputWord.equals(matchedWord) && (inputWord.startsWith(matchedWord) || inputWord.endsWith(matchedWord))) {
            return;
        }

        if (highEditDistance(matchStats, inputWord, matchedWord)) {
            return;
        }

        if (matchStats.editDistance >= 3) {
            if (inputWordEncodings != null) {
                int encodingMatches = 0;
                for (Object e : suggestedWordEncodings) {
                    String encoding = (String) e;
                    if (e != null && !encoding.startsWith(GRAM_PREFIX)
                            && !encoding.startsWith(Constants.GRAM_START_PREFIX)
                            && !encoding.startsWith(Constants.GRAM_END_PREFIX)
                            && inputWordEncodings.contains(e)) {
                        encodingMatches++;
                    }
                }

                if (logger.isDebugEnabled()) {
                    logger.debug("For input: {}, suggested: {}, encoding matches = {}", inputWord, matchedWord, encodingMatches);
                }

                if (encodingMatches == 0) {
                    return;
                }
            }
        }

        boolean goodSuggestion = goodSuggestion(bestStats, matchStats, inputWordLength);

        if (logger.isDebugEnabled()) {
            logger.debug(">>>>> {} suggestion for input: {}, suggested: {}, best: {}, current: {}", goodSuggestion ? "Including" : "Ignoring", inputWord, matchedWord, bestStats, /*previousStats,*/ matchStats);
        }

        if (!goodSuggestion) {
            return;
        }

        if (bestStats != null && bestStats.getMatchLevel().getLevel() >= MatchLevel.Phonetic.getLevel()) {
            buildPhoneticSuggestions(taskContext,
                    matchStats,
                    searchHit,
                    inputTokenType,
                    matchTokenType,
                    matchedWord,
                    suggestionMap);
        } else {
            addSuggestions(taskContext,
                    fieldValues(searchHit, FIELD_STATS),
                    inputTokenType,
                    matchTokenType,
                    matchedWord,
                    matchedWord,
                    display,
                    matchStats,
                    totalCount,
                    suggestionMap);
        }
    }

    @SuppressWarnings("unchecked")
    private void buildPhoneticSuggestions(TaskContext taskContext,
                                          Suggestion.MatchStats matchStats,
                                          SearchHit searchHit,
                                          TokenType inputTokenType,
                                          TokenType matchTokenType,
                                          String matchedWord,
                                          Map<String, Suggestion> suggestionMap) {
        List<Object> originalWordsInfoList = fieldValues(searchHit, FIELD_ORIGINAL_WORDS);

        for (Object originalWordData : originalWordsInfoList) {
            Map<String, Object> originalWordInfo = (Map<String, Object>) originalWordData;
            String originalWord = (String) originalWordInfo.get(FIELD_WORD);
            String originalDisplay = (String) originalWordInfo.get(FIELD_DISPLAY);
            int originalWordCount = (int) originalWordInfo.getOrDefault(FIELD_TOTAL_COUNT, 0);
//            double originalWordWeight = (double) originalWordInfo.getOrDefault(FIELD_TOTAL_WEIGHT, 0.0d);

            if (originalDisplay == null) {
                originalDisplay = originalWord;
            }

            boolean inScope = false;
            List<Object> fieldStatsList = (List<Object>) originalWordInfo.getOrDefault(FIELD_STATS, null);
            if (fieldStatsList != null) {
                for (Object fieldStats : fieldStatsList) {
                    Map<String, Object> fieldStatsData = (Map<String, Object>) fieldStats;
                    String fieldName = (String) fieldStatsData.get(FIELD_FIELD_NAME);

                    if (taskContext.inScope(fieldName)) {
                        inScope = true;
                        break;
                    }
                }
            }

            if (!inScope) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Not adding {} ({}) as not in scope", originalWord, originalDisplay);
                }

                continue;
            }

            addSuggestions(taskContext,
                    fieldStatsList,
                    inputTokenType,
                    matchTokenType,
                    originalWord,
                    matchedWord,
                    originalDisplay,
                    matchStats,
                    originalWordCount,
                    suggestionMap);
        }
    }

    @SuppressWarnings("unchecked")
    private void addSuggestions(TaskContext taskContext,
                                List<Object> fieldStatsList,
                                TokenType inputTokenType,
                                TokenType matchTokenType,
                                String suggestion,
                                String match,
                                String display,
                                Suggestion.MatchStats matchStats,
                                int totalCount,
                                Map<String, Suggestion> suggestionMap) {
        if (fieldStatsList == null) {
            return;
        }

        fieldStatsList
                .stream()
                .map(fieldStats -> (Map<String, Object>) fieldStats)
                .filter(fieldStats -> taskContext.inScope((String) fieldStats.get(FIELD_FIELD_NAME)))
                .map(fieldStats -> {
                    String fieldName = (String) fieldStats.get(FIELD_FIELD_NAME);
                    int fieldCount = (Integer) fieldStats.get(FIELD_TOTAL_COUNT);
                    float occurrence = (fieldCount * 100.0f) / totalCount;

                    if (occurrence < 1.0f) {
                        return null;
                    }

                    SuggestionScope suggestionScope = taskContext.getScope(fieldName);

                    return new Suggestion.EntityType(suggestionScope.getEntityClass(), suggestionScope.getEntityName(), fieldCount, occurrence, suggestionScope.getWeight());
                })
                .filter(entityType -> entityType != null)
                .collect(Collectors.groupingBy(Suggestion.EntityType::getEntityClass))
                .entrySet()
                .forEach(mapEntry -> {
                    Suggestion suggestionObj = new Suggestion(inputTokenType,
                            matchTokenType,
                            suggestion,
                            match,
                            display,
                            matchStats,
                            totalCount,
                            mapEntry.getKey(),
                            mapEntry.getValue());

                    addSuggestion(suggestionObj, suggestionMap);
                });
    }

    private Suggestion.MatchStats buildCandidateStats(String inputWord, String matchedWord, String suggestedWord, float score, List<Object> suggestedWordEncodings, Set<String> inputWordEncodings) {
        int distance = 0;

        MatchLevel matchLevel;
        if (StringUtils.equals(inputWord, matchedWord)) {
            score = 1.0f;
            if (!StringUtils.equals(matchedWord, suggestedWord)) {
                matchLevel = MatchLevel.EdgeGram;
            } else {
                matchLevel = MatchLevel.Exact;
            }
        } else {
            if (!StringUtils.equals(matchedWord, suggestedWord)) {
                matchLevel = MatchLevel.EdgeGramPhonetic;
            } else {
                matchLevel = MatchLevel.Phonetic;
            }
        }

        if (!inputWord.equals(suggestedWord)) {
            // we have exact match here
            // we select the word with proper edit distance
            distance = EditDistanceUtils.getDamerauLevenshteinDistance(inputWord, suggestedWord);

            if (suggestedWordEncodings != null) {
                if (inputWordEncodings != null) {
                    int totalEncodings = suggestedWordEncodings.size() + inputWordEncodings.size();

                    int encodingMatches = 0;
                    for (Object e : suggestedWordEncodings) {
                        String encoding = (String) e;
                        if (inputWordEncodings.contains(encoding)) {
                            encodingMatches++;
                        }
                    }

                    score = 2.0f * encodingMatches / totalEncodings;
                }
            }
        }

        return new Suggestion.MatchStats(matchLevel, distance, score);
    }

    private Suggestion.MatchStats bestCandidateStats(Suggestion.MatchStats... candidateStatsArray) {
        int editDistance = Integer.MAX_VALUE;
        float score = 0f;
        int matchLevel = Integer.MAX_VALUE;

        for (Suggestion.MatchStats candidateStats : candidateStatsArray) {
            editDistance = Math.min(editDistance, candidateStats.editDistance);
            score = Math.max(score, candidateStats.score);
            matchLevel = Math.min(matchLevel, candidateStats.matchLevel.getLevel());
        }

        return new Suggestion.MatchStats(MatchLevel.byLevel(matchLevel), editDistance, score);
    }

    @SuppressWarnings("unchecked")
    private <V> V fieldValue(SearchHit searchHit, String field, V defaultValue) {
//        SearchHitField searchHitField = searchHit.field(field);
//        if (searchHitField != null) {
//            return searchHitField.value();
//        } else {
        Object value = searchHit.getSource().get(field);
        if (value != null) {
            return (V) value;
        }

        return defaultValue;
//        }
    }

    @SuppressWarnings("unchecked")
    private List<Object> fieldValues(SearchHit searchHit, String field) {
//        SearchHitField searchHitField = searchHit.field(field);
//        if (searchHitField != null) {
//            return searchHitField.values();
//        } else {
        return (List<Object>) searchHit.getSource().get(field);
//        }
    }

    @SuppressWarnings("unchecked")
    private Set<Suggestion> wordSuggestions(TaskContext taskContext, String inputWord, TokenType inputTokenType, SearchHit[] searchHits, Set<String> encodings) {
        if (logger.isDebugEnabled()) {
            logger.debug("~~~~~~~~~~~~~~~~ building word suggestions for: {} = {}", inputWord, searchHits);
        }

        if (searchHits == null || searchHits.length == 0) {
            return null;
        }

        int inputWordLength = inputWord.length();

        Map<String, Suggestion> suggestionMap = new HashMap<>();

        int hitsCount = searchHits.length;

        Suggestion.MatchStats[] candidateStatsArray = buildAllCandidateStats(inputWord, searchHits, hitsCount, encodings);

        Suggestion.MatchStats bestCandidateStats = bestCandidateStats(candidateStatsArray);

        if (logger.isDebugEnabled()) {
            logger.debug("Best candidate stats for word: {} = {}", inputWord, bestCandidateStats);
        }

        for (int i = 0; i < hitsCount; i++) {
            SearchHit searchHit = searchHits[i];

            String type = searchHit.type();

            String suggestedWord = fieldValue(searchHit, FIELD_WORD, null);

            String displayWord;

            TokenType matchTokenType;

            if (Constants.UNIGRAM_DID_YOU_MEAN_INDEX_TYPE.equals(type)) {
                displayWord = suggestedWord;
                matchTokenType = TokenType.Uni;
            } else {
                String suggestedWord1 = fieldValue(searchHit, FIELD_WORD_1, null);
                String suggestedWord2 = fieldValue(searchHit, FIELD_WORD_2, null);
                displayWord = suggestedWord1 + " " + suggestedWord2;
                matchTokenType = TokenType.Bi;

//                if (inputWord.length() <= suggestedWord1.length() + Math.min(2, suggestedWord2.length() / 2)) {
//                    logger.info("Bigram length check: input: {}, word1: {}, word2: {}, input#: {}, word1#: {}, word2#: {}, total: {}", inputWord, suggestedWord1, suggestedWord2, inputWord.length(), suggestedWord1.length(), suggestedWord2.length(), suggestedWord1.length() + Math.min(2, suggestedWord2.length() / 2));
//                    continue;
//                }
            }

            buildSuggestion(taskContext,
                    bestCandidateStats,
                    candidateStatsArray[i],
                    searchHit,
                    inputWord,
                    inputTokenType,
                    matchTokenType,
                    suggestedWord,
                    displayWord,
                    inputWordLength,
                    suggestionMap,
                    encodings);
        }

        if (suggestionMap.size() == 0) {
            return null;
        }

        return suggestionSet(inputWord, suggestionMap);
    }

    private Suggestion.MatchStats[] buildAllCandidateStats(String inputWord, SearchHit[] searchHits, int hitsCount, Set<String> inputWordEncodings) {
        Suggestion.MatchStats[] candidateStats = new Suggestion.MatchStats[hitsCount];

        for (int i = 0; i < hitsCount; i++) {
            SearchHit searchHit = searchHits[i];

            String matchedWord = fieldValue(searchHit, FIELD_WORD, null);

            List<Object> suggestedWordEncodings = fieldValues(searchHit, FIELD_ENCODINGS);

            // check for exact suggestion
            candidateStats[i] = buildCandidateStats(inputWord, matchedWord, matchedWord, searchHit.score(), suggestedWordEncodings, inputWordEncodings);
        }

        return candidateStats;
    }

    private SortedSet<Suggestion> suggestionSet(String word, Map<String, Suggestion> suggestionMap) {
        // todo: do this restriction only on a flag = strict, but for now we are doing for all
        boolean hasExactMatch = false;
        boolean hasEdgeGramMatch = false;
        boolean hasPhoneticMatch = false;
        for (Suggestion suggestion : suggestionMap.values()) {
            if (suggestion.getMatchStats().getMatchLevel() == MatchLevel.EdgeGram) {
                hasEdgeGramMatch = true;
            } else if (suggestion.getMatchStats().getMatchLevel() == MatchLevel.Exact) {
                hasExactMatch = true;
            } else if (suggestion.getMatchStats().getMatchLevel() == MatchLevel.Phonetic) {
                hasPhoneticMatch = true;
            }
        }

        SortedSet<Suggestion> suggestions = new TreeSet<>();

        boolean hasSynonyms = addSynonyms(word, suggestions);

        for (Suggestion suggestion : suggestionMap.values()) {
            // TODO: is pruning appropriate
            if ((hasExactMatch || hasEdgeGramMatch) && suggestion.getMatchStats().getMatchLevel().getLevel() > MatchLevel.EdgeGram.getLevel()) {
                continue;
            }

            if (hasSynonyms || hasPhoneticMatch && suggestion.getMatchStats().getMatchLevel().getLevel() > MatchLevel.Phonetic.getLevel()) {
                continue;
            }

            suggestions.add(suggestion);
        }

        // from the final left suggestions
        boolean edgeGram = false;
        boolean fullGram = false;
        if (!hasExactMatch && !hasEdgeGramMatch && !hasSynonyms && hasPhoneticMatch) {
            // find out if we have mix of edgeGram and normal suggestions
            for (Suggestion suggestion : suggestions) {
                edgeGram = edgeGram || suggestion.isEdgeGram();
                fullGram = fullGram || !suggestion.isEdgeGram();
            }

            // we have a mix
            if (edgeGram && fullGram) {
                SortedSet<Suggestion> recomputedSuggestions = new TreeSet<>();
                for (Suggestion suggestion : suggestions) {
                    if (!suggestion.isEdgeGram()) {
                        recomputedSuggestions.add(suggestion);
                    } else {
                        Suggestion.MatchStats recomputedMatchStats = buildCandidateStats(word,
                                suggestion.getMatch(),
                                suggestion.getSuggestion(),
                                suggestion.getMatchStats().getScore(),
                                null,
                                null);

                        suggestion.setMatchStats(recomputedMatchStats);
                        recomputedSuggestions.add(suggestion);
                    }
                }

                Suggestion bestSuggestion = recomputedSuggestions.first();

                int inputWordLength = word.length();

                suggestions = new TreeSet<>();

                // prune
                for (Suggestion suggestion : recomputedSuggestions) {
                    if (highEditDistance(suggestion.getMatchStats(), word, suggestion.getSuggestion())) {
                        suggestion.setIgnore(true);
                    } else if (!goodSuggestion(bestSuggestion.getMatchStats(), suggestion.getMatchStats(), inputWordLength)) {
                        suggestion.setIgnore(true);
                    } else {
                        suggestions.add(suggestion);
                    }
                }
            }
        }

        if (suggestions.size() > 0 && suggestions.first().getMatchStats().getMatchLevel() == MatchLevel.EdgeGramPhonetic && suggestions.first().getInputTokenType() == TokenType.Uni) {
            suggestions
                    .stream()
                    .filter(suggestion -> suggestion.getInputTokenType() == TokenType.Bi)
                    .forEach(suggestion -> suggestion.setIgnore(true));
        }

        if (logger.isDebugEnabled()) {
            logger.debug(">>> Final Suggestions: {}", suggestions);
        }

        return suggestions;
    }

    private boolean addSynonyms(String word, SortedSet<Suggestion> suggestions) {
//        boolean hasSynonyms = false;
//        String[] synonymList = standardSynonyms.get(word);
//        if (synonymList != null) {
//
//            // TODO: see how to populate types for synonyms
//            for (String synonym : synonymList) {
//                Suggestion.MatchStats matchStats = new Suggestion.MatchStats(MatchLevel.Synonym, 0, /*0, 0, 0,*/ 1.0f);
//                suggestions.add(new Suggestion(Suggestion.TokenType.Uni, synonym, synonym, synonym, matchStats, 0, null));
//                hasSynonyms = true;
//            }
//        }
//        return hasSynonyms;
        return false;
    }

    private void addSuggestion(Suggestion suggestion, Map<String, Suggestion> suggestionMap) {
        if (logger.isDebugEnabled()) {
            logger.debug("Adding suggestion: {}", suggestion);
        }

        String key = suggestion.key();
        if (suggestionMap.containsKey(key)) {
            Suggestion existingSuggestion = suggestionMap.get(key);
            int ret = suggestion.compareTo(existingSuggestion);
            if (ret >= 0) {
                if (logger.isDebugEnabled()) {
                    logger.debug("+++++++++++++++++++ Not adding suggestion {} as better {}", suggestion, existingSuggestion);
                }
                return;
            }
        }

        suggestionMap.put(key, suggestion);
    }

    private static <T> CompletableFuture<List<T>> sequence(List<CompletableFuture<T>> futures) {
        CompletableFuture<Void> allDoneFuture =
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
        return allDoneFuture.thenApply(v ->
                futures.stream().
                        map(CompletableFuture::join).
                        collect(Collectors.toList())
        );
    }

    static class TaskContext {
        final Client client;
        final String[] indices;
        final Set<? extends SuggestionScope> suggestionScopes;
        final Map<String, ? extends SuggestionScope> suggestionScopeMap;
        final String id;

        final List<String> queryKeys = new ArrayList<>();
        final List<CompletableFuture<SuggestionSet>> suggestionResponses = new ArrayList<>();

        public TaskContext(Client client, String[] indices, Set<? extends SuggestionScope> suggestionScopes) {
            this.client = client;
            this.indices = indices;
            this.suggestionScopes = suggestionScopes;

            if (suggestionScopes != null) {
                suggestionScopeMap = this.suggestionScopes.stream().collect(Collectors.toMap(SuggestionScope::getEntityName, v -> v));
                id = StringUtils.join(this.indices, "+") + "/"
                        + this.suggestionScopes.stream().map(SuggestionScope::getEntityName).collect(Collectors.joining("+"));
            } else {
                suggestionScopeMap = null;
                id = StringUtils.join(this.indices, "+");
            }
        }

        public boolean inScope(String name) {
            return (suggestionScopeMap != null && suggestionScopeMap.containsKey(name));
        }

        public SuggestionScope getScope(String name) {
            if (suggestionScopeMap != null) {
                return suggestionScopeMap.get(name);
            }

            return null;
        }
    }

    static class QueryBuilderHolder {
        final Set<String> encodings;
        final QueryBuilder[] queryBuilders;

        public QueryBuilderHolder(Set<String> encodings, QueryBuilder[] queryBuilders) {
            this.encodings = encodings;
            this.queryBuilders = queryBuilders;
        }
    }
}

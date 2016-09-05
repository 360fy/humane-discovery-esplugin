package io.threesixtyfy.humaneDiscovery.didYouMean.commons;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.lucene.search.spell.LevensteinDistance;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.threesixtyfy.humaneDiscovery.didYouMean.commons.Constants.GRAM_END_PREFIX_LENGTH;
import static io.threesixtyfy.humaneDiscovery.didYouMean.commons.Constants.GRAM_PREFIX;
import static io.threesixtyfy.humaneDiscovery.didYouMean.commons.Constants.GRAM_PREFIX_LENGTH;
import static io.threesixtyfy.humaneDiscovery.didYouMean.commons.Constants.GRAM_START_PREFIX_LENGTH;
import static io.threesixtyfy.humaneDiscovery.didYouMean.commons.MatchLevel.EdgeGram;
import static io.threesixtyfy.humaneDiscovery.didYouMean.commons.MatchLevel.EdgeGramPhonetic;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;

public class SpellSuggestionsBuilder {

    public static final float GRAM_START_BOOST = 5.0f;
    public static final float GRAM_END_BOOST = 5.0f;
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
    public static final String[] FETCH_SOURCES = new String[]{FIELD_ENCODINGS, FIELD_TOTAL_WEIGHT, FIELD_TOTAL_COUNT, FIELD_COUNT_AS_FULL_WORD, FIELD_WORD, FIELD_WORD_1, FIELD_WORD_2, "originalWords.*"};
    public static final String FIELD_WORD_1_ENCODINGS = "word1Encodings";
    public static final String FIELD_WORD_2_ENCODINGS = "word2Encodings";
    public static final String FIELD_STATS = "fieldStats";
    public static final String FIELD_FIELD_NAME = "fieldName";
    public static final String FIELD_TYPE_NAME = "typeName";

    private final ESLogger logger = Loggers.getLogger(SpellSuggestionsBuilder.class);

    private final CompletableFuture<SuggestionSet> NumberSuggestion = new CompletableFuture<>();

    private final CompletableFuture<SuggestionSet> SingleLetterSuggestion = new CompletableFuture<>();

    private StandardSynonyms standardSynonyms = new StandardSynonyms();

    private final EncodingsBuilder encodingsBuilder = new EncodingsBuilder();

    private final LevensteinDistance levensteinDistance = new LevensteinDistance();

    // TODO: expire on event only
    private final Cache<String, CompletableFuture<SuggestionSet>> CachedCompletableResponses = CacheBuilder
            .newBuilder()
            .maximumSize(1000)
            .expireAfterAccess(30, TimeUnit.SECONDS)
            .build();

    private final ExecutorService futureExecutorService = new ThreadPoolExecutor(5, 5, 30, TimeUnit.SECONDS, new LinkedBlockingQueue<>(100));

    public SpellSuggestionsBuilder() {
        NumberSuggestion.complete(new SuggestionSet(true, false, null));

        SingleLetterSuggestion.complete(new SuggestionSet(false, false, new Suggestion[0]));
    }

    public Map<String, SuggestionSet> fetchSuggestions(Client client, Collection<Conjunct> conjuncts, String[] indices, String[] queryTypes, String[] queryFields) {
        TaskContext taskContext = new TaskContext(client, indices, queryTypes, queryFields);

        try {
            return fetchSuggestions(taskContext, conjuncts);
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Error in fetching suggestions for conjuncts: {}, indices: {}", e, conjuncts, indices);
        }

        return null;
    }

    private CompletableFuture<SuggestionSet> store(TaskContext taskContext, String key, String word, CompletableFuture<SuggestionSet> future) {
        taskContext.queryKeys.add(key);
        taskContext.suggestionResponses.add(future);

        return future;
    }

    private CompletableFuture<SuggestionSet> getOrBuildFuture(TaskContext taskContext, String key, String word, boolean compound, Function<Boolean, QueryBuilder> builder) throws ExecutionException {
        boolean number = NumberUtils.isNumber(word);

        // we do not store these... as re-calculating them would not be that costly
        if (number) {
            return store(taskContext, key, word, NumberSuggestion);
        }

        boolean stopWord = StopWords.contains(word);

        if (word.length() == 1) {
            return store(taskContext, key, word, SingleLetterSuggestion);
        }

        CompletableFuture<SuggestionSet> future = CachedCompletableResponses.get(key(taskContext, key), () -> future(taskContext, key, word, compound, stopWord, builder.apply(stopWord)));

        return store(taskContext, key, word, future);
    }

    private CompletableFuture<SuggestionSet> future(final TaskContext taskContext, String key, String word, boolean compoundWord, boolean stopWord, QueryBuilder queryBuilder) {
        if (logger.isDebugEnabled()) {
            logger.debug("Building future for key: {}, word: {} indices: {}, query: {}", key, word, taskContext.indices, queryBuilder);
        }

        return CompletableFuture
                .supplyAsync(() ->
                                taskContext.client.prepareSearch(taskContext.indices)
                                        .setSize(25)
                                        .setQuery(queryBuilder)
//                                            .addFieldDataField("encodings")
//                                            .addField("totalWeight")
//                                            .addField("totalCount")
//                                            .addField("countAsFullWord")
//                                            .addFieldDataField("word")
//                                            .addFieldDataField("word1")
//                                            .addFieldDataField("word2")
                                        .setFetchSource(FETCH_SOURCES, null)
                                        .execute()
                                        .actionGet(500, TimeUnit.MILLISECONDS),
                        futureExecutorService)
                .exceptionally(error -> {
                    logger.error("Error in executing future for key: {}, word: {} indices: {}", error, key, word, taskContext.indices);
                    return null;
                })
                .thenApply(searchResponse -> {
                    boolean ignorePrefixSuggestions = key.endsWith("/compoundUni") || key.endsWith("/compoundBi");

                    Set<Suggestion> suggestions = wordSuggestions(taskContext, word, compoundWord, searchResponse, ignorePrefixSuggestions);

                    return new SuggestionSet(false, stopWord, suggestions == null ? null : suggestions.toArray(new Suggestion[suggestions.size()]));
                });
    }

    private void addScope(TaskContext taskContext, BoolQueryBuilder boolQueryBuilder) {
        BoolQueryBuilder scopeQueryBuilder = new BoolQueryBuilder();
        for (String typeName : taskContext.queryTypes) {
            for (String fieldName : taskContext.queryFields) {

                scopeQueryBuilder.should(QueryBuilders.nestedQuery("fieldStats",
                        new BoolQueryBuilder()
                                .must(QueryBuilders.termQuery("fieldStats.typeName", typeName))
                                .must(QueryBuilders.termQuery("fieldStats.fieldName", fieldName))));
            }
        }

        scopeQueryBuilder.minimumNumberShouldMatch(1);

        boolQueryBuilder.filter(scopeQueryBuilder);
    }

    private void unigramWordMatch(TaskContext taskContext, Conjunct conjunct, boolean onlyFullWord) throws ExecutionException {
        String key = conjunct.getKey();
        String word = conjunct.getTokens().get(0);

        getOrBuildFuture(taskContext, key, word, false, (stopWord) -> {
            Set<String> encodings = encodingsBuilder.encodings(word, stopWord);

            if (logger.isDebugEnabled()) {
                logger.debug("Encoding for word {} = {}", word, encodings);
            }

            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder()
                    .should(buildWordQuery(Constants.UNIGRAM_DID_YOU_MEAN_INDEX_TYPE, FIELD_ENCODINGS, word, encodings, onlyFullWord))
                    .should(buildWordQuery(Constants.BIGRAM_DID_YOU_MEAN_INDEX_TYPE, FIELD_ENCODINGS, word, encodings, onlyFullWord))
                    .minimumNumberShouldMatch(1);

            addScope(taskContext, boolQueryBuilder);

            return boolQueryBuilder;
        });
    }

    @SuppressWarnings("unchecked")
    private void compoundWordMatch(TaskContext taskContext, Conjunct conjunct, boolean onlyFullWord) throws ExecutionException {
        String word = StringUtils.join(conjunct.getTokens(), "");
        String key = conjunct.getKey();

        getOrBuildFuture(taskContext, key, word, true, (stopWord) -> {
            Set<String> encodings = encodingsBuilder.encodings(word, stopWord);

            if (logger.isDebugEnabled()) {
                logger.debug("Encoding for word {} = {}", word, encodings);
            }

            QueryBuilder shingleQueryBuilder = null;
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

                shingleQueryBuilder = boolQuery()
                        .must(buildWordQuery(Constants.BIGRAM_DID_YOU_MEAN_INDEX_TYPE, FIELD_WORD_1_ENCODINGS, word1, word1Encodings, onlyFullWord))
                        .must(buildWordQuery(Constants.BIGRAM_DID_YOU_MEAN_INDEX_TYPE, FIELD_WORD_2_ENCODINGS, word2, word2Encodings, onlyFullWord));
            }

            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder()
                    .should(buildWordQuery(Constants.UNIGRAM_DID_YOU_MEAN_INDEX_TYPE, FIELD_ENCODINGS, word, encodings, onlyFullWord))
                    .should(buildWordQuery(Constants.BIGRAM_DID_YOU_MEAN_INDEX_TYPE, FIELD_ENCODINGS, word, encodings, onlyFullWord))
                    .minimumNumberShouldMatch(1);

            if (shingleQueryBuilder != null) {
                boolQueryBuilder.should(shingleQueryBuilder);
            }

            addScope(taskContext, boolQueryBuilder);

            return boolQueryBuilder;
        });
    }

    private String key(TaskContext taskContext, String key) {
        return taskContext.id + "/" + key;
    }

    public Map<String, SuggestionSet> fetchSuggestions(TaskContext taskContext, Collection<Conjunct> conjuncts) throws ExecutionException, InterruptedException {
        if (conjuncts == null || conjuncts.size() == 0) {
            return null;
        }

        long startTime = System.currentTimeMillis();

        int size = conjuncts.size();
        int i = 1;
        for (Conjunct conjunct : conjuncts) {
            if (conjunct.getLength() == 1) {
                unigramWordMatch(taskContext, conjunct, i < size);
            } else {
                compoundWordMatch(taskContext, conjunct, i < size);
            }

            i++;
        }

        CompletableFuture<List<SuggestionSet>> allResponses = sequence(taskContext.suggestionResponses);

        if (logger.isDebugEnabled()) {
            logger.debug("For conjuncts: {} and indices: {} build completable search responses: {} in {}ms", conjuncts, taskContext.indices, taskContext.suggestionResponses, (System.currentTimeMillis() - startTime));
        }

        startTime = System.currentTimeMillis();

        Map<String, SuggestionSet> suggestionsMap = allResponses.thenApply(responses -> {
            Map<String, SuggestionSet> map = new HashMap<>();

            int index = 0;
            for (SuggestionSet suggestions : responses) {
                map.put(taskContext.queryKeys.get(index), suggestions);
                index++;
            }

            return map;
        }).get();

        if (logger.isDebugEnabled()) {
            logger.debug("For conjuncts: {} and indices: {} created suggestions: {} in {}ms", conjuncts, taskContext.indices, suggestionsMap, (System.currentTimeMillis() - startTime));
        }

        return suggestionsMap;
    }

    private BoolQueryBuilder buildWordQuery(String type, String field, String word, Set<String> phoneticEncodings, boolean onlyFullWord) {
        BoolQueryBuilder boolQueryBuilder = boolQuery()
                .filter(QueryBuilders.typeQuery(type))
                .should(QueryBuilders.termQuery(field, word).boost(EXACT_TERM_BOOST));

//        if (onlyFullWord) {
//            boolQueryBuilder.must(QueryBuilders.rangeQuery("countAsFullWord").from(1));
//        }

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

    private boolean highEditDistance(Suggestion.MatchStats currentStats, String inputWord, String suggestedWord) {
        boolean isHigh = currentStats.editDistance >= 5 /* || currentStats.lDistance < 0.5 || currentStats.jwDistance < 0.5*/;

        if (isHigh) {
            if (logger.isDebugEnabled()) {
                logger.info("High edit distance for input: {}, suggested: {}, stats: {}", inputWord, suggestedWord, currentStats);
            }
        }

        return isHigh;
    }

    private boolean goodSuggestion(Suggestion.MatchStats bestStats, Suggestion.MatchStats previousStats, Suggestion.MatchStats currentStats, int inputWordLength) {
        if (bestStats != null) {
            if (inputWordLength <= 4 && currentStats.editDistance / inputWordLength > 0.40) {
                return false;
            }

            if (inputWordLength > 4 && currentStats.editDistance / inputWordLength > 0.50) {
                return false;
            }

            // we relax at the most one edit distance from the best match
            if (bestStats.editDistance + 1 < currentStats.editDistance) {
                return false;
            }

//            // more than 40% drop in lDistance -- earlier 25%
//            if ((bestStats.lDistance - currentStats.lDistance) / currentStats.lDistance > 0.40) {
//                return false;
//            }
//
//            // more than 40% drop in jwDistance -- earlier 25%
//            if ((bestStats.jwDistance - currentStats.jwDistance) / currentStats.jwDistance > 0.40) {
//                return false;
//            }
//
//            // more than 5 times drop from best score -- earlier 3 times
//            if (bestStats.score / currentStats.score > 5.0) {
//                return false;
//            }
//
//            // more than 3.0 drop from previous score -- earlier 2 times
//            if (previousStats != null && bestStats.score != previousStats.score && previousStats.score / currentStats.score > 3.0) {
//                return false;
//            }
        }

        return true;
    }

    @SuppressWarnings("unchecked")
    private void buildSuggestion(TaskContext taskContext,
                                 Suggestion.MatchStats bestStats,
                                 Suggestion.MatchStats previousStats,
                                 Suggestion.MatchStats currentStats,
                                 SearchHit searchHit,
                                 String inputWord,
                                 Suggestion.TokenType tokenType,
                                 String suggestedWord,
                                 String display,
                                 int inputWordLength,
                                 boolean ignorePrefixSuggestions,
                                 Map<String, Suggestion> suggestionMap) {
        double totalWeight = fieldValue(searchHit, FIELD_TOTAL_WEIGHT, 0.0d);
        int totalCount = fieldValue(searchHit, FIELD_TOTAL_COUNT, 0);
        int countAsFullWord = fieldValue(searchHit, FIELD_COUNT_AS_FULL_WORD, 0);
        List<Object> suggestedWordEncodings = fieldValues(searchHit, FIELD_ENCODINGS);

        boolean edgeGram = (countAsFullWord * 100.0 / totalCount) < 40.0;

        // suggested word is prefix of input word
        if (ignorePrefixSuggestions && !inputWord.equals(suggestedWord) && (inputWord.startsWith(suggestedWord) || inputWord.endsWith(suggestedWord))) {
            return;
        }

        // 5 edit distance is too high, isn't it
        if (highEditDistance(currentStats, inputWord, suggestedWord)) {
            return;
        }

        if (currentStats.editDistance >= 3) {
            Set<String> inputWordEncodings = encodingsBuilder.encodings(inputWord, StopWords.contains(inputWord));

            if (inputWordEncodings != null) {
                int encodingMatches = 0;
                for (Object e : suggestedWordEncodings) {
                    String encoding = (String) e;
                    if (e != null && !encoding.startsWith(GRAM_PREFIX) && !encoding.startsWith(Constants.GRAM_START_PREFIX) && !encoding.startsWith(Constants.GRAM_END_PREFIX) && inputWordEncodings.contains(e)) {
                        encodingMatches++;
                    }
                }

                logger.info("For input: {}, suggested: {}, encoding matches = {}", inputWord, suggestedWord, encodingMatches);

                if (encodingMatches == 0) {
                    return;
                }
            }
        }

        boolean goodSuggestion = goodSuggestion(bestStats, previousStats, currentStats, inputWordLength);

        if (logger.isDebugEnabled()) {
            logger.debug(">>>>> {} suggestion for input: {}, suggested: {}, best: {}, previous: {}, current: {}", goodSuggestion ? "Including" : "Ignoring", inputWord, suggestedWord, bestStats, previousStats, currentStats);
        }

        if (!goodSuggestion) {
            return;
        }

        // TODO: if we are adding all original words, why it is needed ???
        if (!edgeGram) {
            addSuggestion(new Suggestion(tokenType,
                            suggestedWord,
                            suggestedWord,
                            display,
                            currentStats,
                            totalWeight,
                            totalCount),
                    suggestionMap);
        }

        List<Object> originalWordsInfoList = fieldValues(searchHit, FIELD_ORIGINAL_WORDS); //searchHit.sourceAsMap().get("originalWords");

        for (Object originalWordData : originalWordsInfoList) {
            Map<String, Object> originalWordInfo = (Map<String, Object>) originalWordData;
            String originalWord = (String) originalWordInfo.get(FIELD_WORD);
            String originalDisplay = (String) originalWordInfo.get(FIELD_DISPLAY);
            int originalWordCount = (int) originalWordInfo.getOrDefault(FIELD_TOTAL_COUNT, 0);
            double originalWordWeight = (double) originalWordInfo.getOrDefault(FIELD_TOTAL_WEIGHT, 0.0d);

            if (originalDisplay == null) {
                originalDisplay = originalWord;
            }

            boolean inScope = false;
            List<Object> fieldStatsList = (List<Object>) originalWordInfo.getOrDefault(FIELD_STATS, null);
            if (fieldStatsList != null) {
                for (Object fieldStats : fieldStatsList) {
                    Map<String, Object> fieldStatsData = (Map<String, Object>) fieldStats;
                    String typeName = (String) fieldStatsData.get(FIELD_TYPE_NAME);
                    String fieldName = (String) fieldStatsData.get(FIELD_FIELD_NAME);

                    for (String queryType : taskContext.queryTypes) {
                        if (!StringUtils.equals(typeName, queryType)) {
                            continue;
                        }

                        for (String queryField : taskContext.queryFields) {
                            if (StringUtils.equals(fieldName, queryField)) {
                                inScope = true;
                                break;
                            }
                        }

                        if (inScope) {
                            break;
                        }
                    }
                }
            }

            if (!inScope) {
                continue;
            }

            if (StringUtils.equals(originalDisplay, suggestedWord)) {
                continue;
            }

            Suggestion.MatchStats originalWordStats = currentStats;
            if (bestStats != null
                    && bestStats.getMatchLevel().getLevel() <= MatchLevel.Phonetic.getLevel()
                    && bestStats.getEditDistance() >= currentStats.getEditDistance()
                    && bestStats.getScore() > currentStats.getScore()) {
                // calculate current stats
                originalWordStats = buildCandidateStats(inputWord, suggestedWord, originalDisplay, 0);

                if (originalWordStats.getMatchLevel() == MatchLevel.EdgeGramPhonetic) {
                    // 5 edit distance is too high, isn't it
                    if (highEditDistance(originalWordStats, inputWord, suggestedWord)) {
                        continue;
                    }

                    if (!goodSuggestion(bestStats, previousStats, originalWordStats, inputWordLength)) {
                        continue;
                    }
                }
            } else {
                // TODO: change the match type to phonetic edge gram...
            }

            // we count similarity with edgeGram
            addSuggestion(new Suggestion(tokenType,
                            originalWord,
                            suggestedWord,
                            originalDisplay,
                            originalWordStats,
                            originalWordWeight,
                            originalWordCount),
                    suggestionMap);
        }
    }

    private Suggestion.MatchStats buildCandidateStats(String inputWord, String matchedWord, String suggestedWord, float score) {
        int distance = 0;
        int similarity = 0;

        MatchLevel matchLevel;
        if (StringUtils.equals(inputWord, matchedWord)) {
            if (matchedWord.length() < suggestedWord.length()) {
                matchLevel = MatchLevel.EdgeGram;
            } else {
                matchLevel = MatchLevel.Exact;
            }
        } else {
            if (matchedWord.length() < suggestedWord.length()) {
                matchLevel = MatchLevel.EdgeGramPhonetic;
            } else {
                matchLevel = MatchLevel.Phonetic;
            }
        }

        if (!inputWord.equals(suggestedWord)) {
            // we have exact match here
            // we select the word with proper edit distance
            distance = EditDistanceUtils.getDamerauLevenshteinDistance(inputWord, suggestedWord);
//            similarity = EditDistanceUtils.getFuzzyDistance(inputWord, suggestedWord, Locale.ENGLISH);
        }

//        double jwDistance = StringUtils.getJaroWinklerDistance(inputWord, suggestedWord);
//        double lDistance = levensteinDistance.getDistance(inputWord, suggestedWord);

        return new Suggestion.MatchStats(matchLevel, distance, similarity, /*jwDistance*/ 0, /*lDistance*/ 0, score);
    }

    private Suggestion.MatchStats bestCandidateStats(Suggestion.MatchStats... candidateStatsArray) {
        int editDistance = Integer.MAX_VALUE;
        int similarity = 0;

        double jwDistance = 0;
        double lDistance = 0;
        float score = 0f;
        int matchLevel = Integer.MAX_VALUE;

        for (Suggestion.MatchStats candidateStats : candidateStatsArray) {
            editDistance = Math.min(editDistance, candidateStats.editDistance);
            similarity = Math.max(similarity, candidateStats.similarity);
            score = Math.max(score, candidateStats.score);
            jwDistance = Math.max(jwDistance, candidateStats.jwDistance);
            lDistance = Math.max(lDistance, candidateStats.lDistance);
            matchLevel = Math.min(matchLevel, candidateStats.matchLevel.getLevel());
        }

        return new Suggestion.MatchStats(MatchLevel.byLevel(matchLevel), editDistance, similarity, jwDistance, lDistance, score);
    }

    @SuppressWarnings("unchecked")
    private <V> V fieldValue(SearchHit searchHit, String field, V defaultValue) {
        SearchHitField searchHitField = searchHit.field(field);
        if (searchHitField != null) {
            return searchHitField.value();
        } else {
            Object value = searchHit.getSource().get(field);
            if (value != null) {
                return (V) value;
            }

            return defaultValue;
        }

//        throw new IllegalArgumentException("Field value not found for: " + field);
    }

    @SuppressWarnings("unchecked")
    private List<Object> fieldValues(SearchHit searchHit, String field) {
        SearchHitField searchHitField = searchHit.field(field);
        if (searchHitField != null) {
            return searchHitField.values();
        } else {
            return (List<Object>) searchHit.getSource().get(field);
        }

//        throw new IllegalArgumentException("Field values not found for: " + field);
    }

    @SuppressWarnings("unchecked")
    private Set<Suggestion> wordSuggestions(TaskContext taskContext, String inputWord, boolean compoundWord, SearchResponse searchResponse, boolean ignorePrefixSuggestions) {
        logger.info("~~~~~~~~~~~~~~~~ building word suggestions for: {} = {}", inputWord, searchResponse);

        if (searchResponse == null || searchResponse.getHits() == null || searchResponse.getHits().getHits() == null) {
            return null;
        }

        int inputWordLength = inputWord.length();

        Map<String, Suggestion> suggestionMap = new HashMap<>();

        int hitsCount = searchResponse.getHits().getHits().length;

        Suggestion.MatchStats[] candidateStatsArray = buildAllCandidateStats(inputWord, searchResponse, hitsCount);

        Suggestion.MatchStats bestCandidateStats = bestCandidateStats(candidateStatsArray);

        logger.info("Best candidate stats for word: {} = {}", inputWord, bestCandidateStats);

        for (int i = 0; i < hitsCount; i++) {
            SearchHit searchHit = searchResponse.getHits().getHits()[i];

            String type = searchHit.type();

            String suggestedWord = fieldValue(searchHit, FIELD_WORD, null);

            String displayWord;

            Suggestion.TokenType tokenType;

            if (Constants.UNIGRAM_DID_YOU_MEAN_INDEX_TYPE.equals(type)) {
                displayWord = suggestedWord;
                tokenType = compoundWord ? Suggestion.TokenType.ShingleUni : Suggestion.TokenType.Uni;
            } else {
                String suggestedWord1 = fieldValue(searchHit, FIELD_WORD_1, null);
                String suggestedWord2 = fieldValue(searchHit, FIELD_WORD_2, null);
                displayWord = suggestedWord1 + " " + suggestedWord2;
                tokenType = compoundWord ? Suggestion.TokenType.ShingleBi : Suggestion.TokenType.Bi;

//                if (inputWord.length() <= suggestedWord1.length() + Math.min(2, suggestedWord2.length() / 2)) {
//                    logger.info("Bigram length check: input: {}, word1: {}, word2: {}, input#: {}, word1#: {}, word2#: {}, total: {}", inputWord, suggestedWord1, suggestedWord2, inputWord.length(), suggestedWord1.length(), suggestedWord2.length(), suggestedWord1.length() + Math.min(2, suggestedWord2.length() / 2));
//                    continue;
//                }
            }

            buildSuggestion(taskContext,
                    bestCandidateStats,
                    i > 0 ? candidateStatsArray[i - 1] : null,
                    candidateStatsArray[i],
                    searchHit,
                    inputWord,
                    tokenType,
                    suggestedWord,
                    displayWord,
                    inputWordLength,
                    ignorePrefixSuggestions,
                    suggestionMap);
        }

        if (suggestionMap.size() == 0) {
            return null;
        }

        return suggestionSet(inputWord, suggestionMap);
    }

    private Suggestion.MatchStats[] buildAllCandidateStats(String inputWord, SearchResponse searchResponse, int hitsCount) {
        Suggestion.MatchStats[] candidateStats = new Suggestion.MatchStats[hitsCount];

        for (int i = 0; i < hitsCount; i++) {
            SearchHit searchHit = searchResponse.getHits().getHits()[i];

            String matchedWord = fieldValue(searchHit, FIELD_WORD, null);

            // check for exact suggestion
            candidateStats[i] = buildCandidateStats(inputWord, matchedWord, matchedWord, searchHit.score());
        }

        return candidateStats;
    }

    private SortedSet<Suggestion> suggestionSet(String word, Map<String, Suggestion> suggestionMap) {
        // todo: do this restriction only on a flag = strict, but for now we are doing for all
        boolean hasExactMatch = false;
        boolean hasEdgeGramMatch = false;
        boolean hasPhoneticMatch = false;
        for (Suggestion suggestion : suggestionMap.values()) {
            if (suggestion.getMatchStats().getMatchLevel() == EdgeGram) {
                hasEdgeGramMatch = true;
            } else if (suggestion.getMatchStats().getMatchLevel() == MatchLevel.Exact) {
                hasExactMatch = true;
            } else if (suggestion.getMatchStats().getMatchLevel() == MatchLevel.Phonetic) {
                hasPhoneticMatch = true;
            }
        }

        SortedSet<Suggestion> suggestions = new TreeSet<>();

        boolean hasSynonyms = false;
        String[] synonymList = standardSynonyms.get(word);
        if (synonymList != null) {

            for (String synonym : synonymList) {
                // TODO: have special type to capture synonyms
                suggestions.add(new Suggestion(Suggestion.TokenType.Uni, synonym, synonym, synonym, /*MatchLevel.Synonym,*/ null, 0, 0));
                hasSynonyms = true;
            }
        }

        for (Suggestion suggestion : suggestionMap.values()) {
//            if ((hasExactMatch || hasEdgeGramMatch) && suggestion.getMatchLevel().getLevel() > EdgeGram.getLevel()) {
//                continue;
//            }
//
//            if (hasSynonyms || hasPhoneticMatch && suggestion.getMatchLevel().getLevel() > MatchLevel.Phonetic.getLevel()) {
//                continue;
//            }

            suggestions.add(suggestion);
        }

        if (suggestions.size() > 0 && suggestions.first().getMatchStats().getMatchLevel() == EdgeGramPhonetic && suggestions.first().getTokenType() == Suggestion.TokenType.Uni) {
            suggestions
                    .stream()
                    .filter(suggestion -> suggestion.getTokenType() == Suggestion.TokenType.Bi)
                    .forEach(suggestion -> suggestion.setIgnore(true));
        }

        if (logger.isDebugEnabled()) {
            logger.debug(">>> Final Suggestions: {}", suggestions);
        }

        return suggestions;
    }

    private void addSuggestion(Suggestion suggestion, Map<String, Suggestion> suggestionMap) {
        logger.info("Adding suggestion: {}", suggestion);

        String key = suggestion.getSuggestion();
        if (suggestionMap.containsKey(key)) {
            Suggestion existingSuggestion = suggestionMap.get(key);
            int ret = suggestion.compareTo(existingSuggestion);
            if (ret >= 0) {
                logger.debug("+++++++++++++++++++ Not adding suggestion {} as better {}", suggestion, existingSuggestion);
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
        final String[] queryTypes;
        final String[] queryFields;
        final String id;

        final List<String> queryKeys = new ArrayList<>();
        final List<CompletableFuture<SuggestionSet>> suggestionResponses = new ArrayList<>();

        public TaskContext(Client client, String[] indices, String[] queryTypes, String[] queryFields) {
            this.client = client;
            this.indices = indices;
            this.queryTypes = queryTypes;
            this.queryFields = queryFields;
            id = StringUtils.join(this.indices, "+") + "/" + StringUtils.join(this.queryTypes, "+") + "/" + StringUtils.join(this.queryFields, "+");
        }
    }
}

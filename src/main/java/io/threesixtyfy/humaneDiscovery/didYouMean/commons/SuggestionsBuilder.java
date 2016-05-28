package io.threesixtyfy.humaneDiscovery.didYouMean.commons;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.search.spell.LevensteinDistance;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
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

import static io.threesixtyfy.humaneDiscovery.didYouMean.commons.MatchLevel.EdgeGram;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;

public class SuggestionsBuilder {

    private final ESLogger logger = Loggers.getLogger(SuggestionsBuilder.class);

    private static final SuggestionsBuilder instance = new SuggestionsBuilder();

    private final Map<Character, Character> similarCharacterMap = new HashMap<>();

    private final LevensteinDistance levensteinDistance = new LevensteinDistance();

    private final Set<String> StopWords = new HashSet<>(Arrays.asList(
            /*"a",*/ "an", "and", "are", "as", "at", "be", "but", "by",
            "for", "if", /*"in",*/ "into", "is", "it",
            "no", "not", "of", "on", "or", "such",
            "that", "the", "their", "then", "there", "these",
            "they", "this", "to", "was", "will", "with"
    ));

    private final CompletableFuture<SuggestionSet> NumberSuggestion = new CompletableFuture<>();

    private final CompletableFuture<SuggestionSet> SingleLetterSuggestion = new CompletableFuture<>();

    private final Cache<String, CompletableFuture<SuggestionSet>> CachedCompletableResponses = CacheBuilder
            .newBuilder()
            .maximumSize(1000)
            .expireAfterAccess(30, TimeUnit.SECONDS)
            .build();

    private final Cache<String, Set<String>> CachedEncodings = CacheBuilder
            .newBuilder()
            .maximumSize(1000)
            .build();

    private final ExecutorService futureExecutorService = new ThreadPoolExecutor(5, 5, 30, TimeUnit.SECONDS, new LinkedBlockingQueue<>(100));

    private int currentEncodingUtilIndex = 0;

    private final int TotalEncodingUtils = 5;

    private final PhoneticEncodingUtils[] phoneticEncodingUtilsArray = new PhoneticEncodingUtils[]{
            new PhoneticEncodingUtils(),
            new PhoneticEncodingUtils(),
            new PhoneticEncodingUtils(),
            new PhoneticEncodingUtils(),
            new PhoneticEncodingUtils()
    };

    private SuggestionsBuilder() {
        NumberSuggestion.complete(new SuggestionSet(true, false, null));

        SingleLetterSuggestion.complete(new SuggestionSet(false, false, new Suggestion[0]));

        similarCharacterMap.put('j', 'z');
        similarCharacterMap.put('z', 'j');
    }

    public static SuggestionsBuilder INSTANCE() {
        return instance;
    }

    public List<String> tokens(AnalysisService analysisService, String query) throws IOException {
        Analyzer analyzer = analysisService.analyzer("humane_query_analyzer");
        if (analyzer == null) {
            throw new RuntimeException("No humane_query_analyzer found");
        }

        TokenStream tokenStream = analyzer.tokenStream("dummyField", query);

        tokenStream.reset();

        CharTermAttribute termAttribute = tokenStream.getAttribute(CharTermAttribute.class);

        List<String> words = new ArrayList<>();
        while (tokenStream.incrementToken()) {
            words.add(termAttribute.toString());
        }

        tokenStream.close();

        return words;
    }

    private Set<String> encodings(String token, boolean stopWord) {
        try {
            return CachedEncodings.get(token, () -> {
                if (logger.isDebugEnabled()) {
                    logger.debug("Building encoding for token: {}", token);
                }

                final PhoneticEncodingUtils phoneticEncodingUtils = phoneticEncodingUtilsArray[currentEncodingUtilIndex++ % TotalEncodingUtils];
                synchronized (phoneticEncodingUtils) {
                    return phoneticEncodingUtils.buildEncodings(token, stopWord);
                }
            });
        } catch (ExecutionException e) {
            return null;
        }
    }

    private BoolQueryBuilder buildWordQuery(String type, String field, String word, Set<String> phoneticEncodings) {
        BoolQueryBuilder boolQueryBuilder = boolQuery()
                .filter(QueryBuilders.typeQuery(type))
                .should(QueryBuilders.termQuery(field, word).boost(100.0f));

        phoneticEncodings.stream().forEach(w -> {

            TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery(field, w);

            if (w.startsWith("gs#")) {
                termQueryBuilder.boost(5.0f);
            }

            if (w.startsWith("ge#")) {
                termQueryBuilder.boost(2.0f);
            }

            boolQueryBuilder.should(termQueryBuilder);
        });

        int clauses = 1 + phoneticEncodings.size();
        if (clauses >= 2) {
            boolQueryBuilder.minimumNumberShouldMatch(2);
        } else {
            boolQueryBuilder.minimumNumberShouldMatch(clauses);
        }

        return boolQueryBuilder;
    }

    // io <=> eo
    // ee <=> i
    // z <=> j
    // c <=> k
    private int getFuzzyDistance(final CharSequence term, final CharSequence query, final Locale locale) {
        if (term == null || query == null) {
            throw new IllegalArgumentException("Strings must not be null");
        } else if (locale == null) {
            throw new IllegalArgumentException("Locale must not be null");
        }

        // fuzzy logic is case insensitive. We normalize the Strings to lower
        // case right from the start. Turning characters to lower case
        // via Character.toLowerCase(char) is unfortunately insufficient
        // as it does not accept a locale.
        final String termLowerCase = term.toString().toLowerCase(locale);
        final String queryLowerCase = query.toString().toLowerCase(locale);

        // the resulting score
        int score = 0;

        // the position in the term which will be scanned next for potential
        // query character matches
        int termIndex = 0;

        // index of the previously matched character in the term
        int previousMatchingCharacterIndex = Integer.MIN_VALUE;

        for (int queryIndex = 0; queryIndex < queryLowerCase.length(); queryIndex++) {
            final char queryChar = queryLowerCase.charAt(queryIndex);

            boolean termCharacterMatchFound = false;
            for (; termIndex < termLowerCase.length() && !termCharacterMatchFound; termIndex++) {
                final char termChar = termLowerCase.charAt(termIndex);

                if (queryChar == termChar || similarCharacterMap.getOrDefault(queryChar, queryChar) == termChar) {
                    // simple character matches result in one point
                    score++;

                    // subsequent character matches further improve
                    // the score.
                    if (previousMatchingCharacterIndex + 1 == termIndex) {
                        score += 2;
                    }

                    previousMatchingCharacterIndex = termIndex;

                    // we can leave the nested loop. Every character in the
                    // query can match at most one character in the term.
                    termCharacterMatchFound = true;
                }
            }
        }

        return score;
    }

    static class CandidateStats {
        int editDistance;
        int similarity;
        double jwDistance;
        double lDistance;
        float score;

        public CandidateStats(int editDistance, int similarity, double jwDistance, double lDistance, float score) {
            this.editDistance = editDistance;
            this.similarity = similarity;
            this.jwDistance = jwDistance;
            this.lDistance = lDistance;
            this.score = score;
        }

        @Override
        public String toString() {
            return "{" +
                    "editDistance=" + editDistance +
                    ", similarity=" + similarity +
                    ", jwDistance=" + jwDistance +
                    ", lDistance=" + lDistance +
                    ", score=" + score +
                    '}';
        }
    }

    @SuppressWarnings("unchecked")
    private void buildSuggestion(CandidateStats bestStats,
                                 CandidateStats previousStats,
                                 CandidateStats currentStats,
                                 Map<String, Object> source,
                                 String inputWord,
                                 Suggestion.TokenType tokenType,
                                 String suggestedWord,
                                 List<String> suggestedWordEncodings,
                                 String display,
                                 int inputWordLength,
                                 boolean ignorePrefixSuggestions,
                                 Map<String, Suggestion> suggestionMap) {
        double totalWeight = (double) source.get("totalWeight");
        int totalCount = (int) source.get("totalCount");
        int countAsFullWord = (int) source.get("countAsFullWord");

        boolean edgeGram = (countAsFullWord * 100.0 / totalCount) < 40.0;
        MatchLevel matchLevel;

        // suggested word is prefix of input word
        if (ignorePrefixSuggestions && !inputWord.equals(suggestedWord) && (inputWord.startsWith(suggestedWord) || inputWord.endsWith(suggestedWord))) {
            return;
        }

        // 5 edit distance is too high, isn't it
        if (currentStats.editDistance >= 5 || currentStats.lDistance < 0.5 || currentStats.jwDistance < 0.5) {
            return;
        }

        if (currentStats.editDistance >= 3) {
            Set<String> inputWordEncodings = encodings(inputWord, StopWords.contains(inputWord));

            if (inputWordEncodings != null) {
                int encodingMatches = 0;
                for (String e : suggestedWordEncodings) {
                    if (e != null && !e.startsWith("g#") && !e.startsWith("gs#") && !e.startsWith("ge#") && inputWordEncodings.contains(e)) {
                        encodingMatches++;
                    }
                }

                if (encodingMatches == 0) {
                    return;
                }
            }
        }

        if (logger.isDebugEnabled()) {
            logger.debug(">>>>> Building suggestion for input: {}, suggested: {}, best: {}, previous: {}, current: {}", inputWord, suggestedWord, bestStats, previousStats, currentStats);
        }

        if (bestStats != null) {
            if (inputWordLength <= 4 && currentStats.editDistance / inputWordLength > 0.40) {
                return;
            }

            if (inputWordLength > 4 && currentStats.editDistance / inputWordLength > 0.50) {
                return;
            }

            // we relax at the most one edit distance from the best match
            if (bestStats.editDistance + 1 < currentStats.editDistance) {
                return;
            }

            // more than 25% drop in lDistance
            if ((bestStats.lDistance - currentStats.lDistance) / currentStats.lDistance > 0.25) {
                return;
            }

            // more than 25% drop in jwDistance
            if ((bestStats.jwDistance - currentStats.jwDistance) / currentStats.jwDistance > 0.25) {
                return;
            }

            // more than 3 times drop from best score
            if (bestStats.score / currentStats.score > 3.0) {
                return;
            }

            // more than 2.0 drop from previous score
            if (previousStats != null && bestStats.score != previousStats.score && previousStats.score / currentStats.score > 2.0) {
                return;
            }
        }

        if (logger.isDebugEnabled()) {
            logger.debug("<<<<< Included suggestion for input: {}, suggested: {}, best: {}, previous: {}, current: {}", inputWord, suggestedWord, bestStats, previousStats, currentStats);
        }

        // TODO: if we are adding all original words, why it is needed ???
        if (!edgeGram) {
            if (StringUtils.equals(suggestedWord, inputWord)) {
                matchLevel = MatchLevel.Exact;
            } else if (suggestedWord.startsWith(inputWord)) {
                matchLevel = EdgeGram;
            } else {
                matchLevel = MatchLevel.Phonetic;
            }

            addSuggestion(new Suggestion(tokenType,
                            suggestedWord,
                            suggestedWord,
                            display,
                            matchLevel,
                            currentStats.editDistance,
                            currentStats.similarity,
                            currentStats.jwDistance,
                            currentStats.lDistance,
                            currentStats.score,
                            totalWeight,
                            totalCount),
                    suggestionMap);
        }

        List<Map<String, Object>> originalWordsInfoList = (List<Map<String, Object>>) source.get("originalWords");
//        int originalWordListSize = originalWordsInfoList.size();
//        int suggestedWordLength = suggestedWord.length();

        for (Map<String, Object> originalWordInfo : originalWordsInfoList) {
            String originalWord = (String) originalWordInfo.get("word");
            String originalDisplay = (String) originalWordInfo.get("display");
            int originalWordCount = (int) originalWordInfo.get("totalCount");
            double originalWordWeight = (double) originalWordInfo.get("totalWeight");

            if (StringUtils.equals(originalWord, inputWord)) {
                matchLevel = MatchLevel.Exact;
            } else if (originalWord.startsWith(inputWord)) {
                matchLevel = EdgeGram;
            } else if (edgeGram) {
                matchLevel = MatchLevel.EdgeGramPhonetic;
            } else {
                matchLevel = MatchLevel.Phonetic;
            }

            // we count similarity with edgeGram
            addSuggestion(new Suggestion(tokenType,
                            originalWord,
                            suggestedWord,
                            originalDisplay == null ? originalWord : originalDisplay,
                            matchLevel,
                            currentStats.editDistance /*+ (originalWordLength - suggestedWordLength)*/,
                            currentStats.similarity,
                            currentStats.jwDistance,
                            currentStats.lDistance,
                            currentStats.score,
                            originalWordWeight,
                            originalWordCount),
                    suggestionMap);
        }
    }

    private CandidateStats buildCandidateStats(String inputWord, String suggestedWord, float score) {
        int distance = 0;
        int similarity = 0;

        if (!inputWord.equals(suggestedWord)) {
            // we have exact match here
            // we select the word with proper edit distance
            distance = StringUtils.getLevenshteinDistance(inputWord, suggestedWord);
            similarity = getFuzzyDistance(inputWord, suggestedWord, Locale.ENGLISH);
        }

        double jwDistance = StringUtils.getJaroWinklerDistance(inputWord, suggestedWord);
        double lDistance = levensteinDistance.getDistance(inputWord, suggestedWord);

        return new CandidateStats(distance, similarity, jwDistance, lDistance, score);
    }

    private CandidateStats bestCandidateStats(CandidateStats... candidateStatsArray) {
        int editDistance = Integer.MAX_VALUE;
        int similarity = 0;

        double jwDistance = 0;
        double lDistance = 0;
        float score = 0f;

        for (CandidateStats candidateStats : candidateStatsArray) {
            editDistance = Math.min(editDistance, candidateStats.editDistance);
            similarity = Math.max(similarity, candidateStats.similarity);
            score = Math.max(score, candidateStats.score);
            jwDistance = Math.max(jwDistance, candidateStats.jwDistance);
            lDistance = Math.max(lDistance, candidateStats.lDistance);
        }

        return new CandidateStats(editDistance, similarity, jwDistance, lDistance, score);
    }

    @SuppressWarnings("unchecked")
    private Set<Suggestion> wordSuggestions(String inputWord, SearchResponse searchResponse, boolean ignorePrefixSuggestions) {
        if (searchResponse == null || searchResponse.getHits() == null || searchResponse.getHits().getHits() == null) {
            return null;
        }

        int inputWordLength = inputWord.length();

        Map<String, Suggestion> suggestionMap = new HashMap<>();

        int hitsCount = searchResponse.getHits().getHits().length;

        CandidateStats[] candidateStatsArray = buildAllCandidateStats(inputWord, searchResponse, hitsCount);

        CandidateStats bestCandidateStats = bestCandidateStats(candidateStatsArray);

        for (int i = 0; i < hitsCount; i++) {
            SearchHit searchHit = searchResponse.getHits().getHits()[i];

            // check for exact suggestion
            Map<String, Object> source = searchHit.getSource();

            String type = searchHit.type();

            String suggestedWord = (String) source.get("word");
            String displayWord;

            Suggestion.TokenType tokenType;

            if (Constants.UNIGRAM_DID_YOU_MEAN_INDEX_TYPE.equals(type)) {
                displayWord = suggestedWord;
                tokenType = Suggestion.TokenType.Uni;
            } else {
                String suggestedWord1 = (String) source.get("word1");
                String suggestedWord2 = (String) source.get("word2");
                displayWord = suggestedWord1 + " " + suggestedWord2;
                tokenType = Suggestion.TokenType.Bi;

                if (inputWord.length() <= suggestedWord1.length() + Math.min(2, suggestedWord2.length() / 2)) {
                    continue;
                }
            }

            buildSuggestion(bestCandidateStats,
                    i > 0 ? candidateStatsArray[i - 1] : null,
                    candidateStatsArray[i],
                    source,
                    inputWord,
                    tokenType,
                    suggestedWord,
                    (List<String>) source.get("encodings"),
                    displayWord,
                    inputWordLength,
                    ignorePrefixSuggestions,
                    suggestionMap);
        }

        if (suggestionMap.size() == 0) {
            return null;
        }

        return suggestionSet(suggestionMap);
    }

    private CandidateStats[] buildAllCandidateStats(String inputWord, SearchResponse searchResponse, int hitsCount) {
        CandidateStats[] candidateStats = new CandidateStats[hitsCount];

        for (int i = 0; i < hitsCount; i++) {
            SearchHit searchHit = searchResponse.getHits().getHits()[i];

            Map<String, Object> source = searchHit.getSource();

            String suggestedWord = (String) source.get("word");

            // check for exact suggestion
            candidateStats[i] = buildCandidateStats(inputWord, suggestedWord, searchHit.score());
        }

        return candidateStats;
    }

    private SortedSet<Suggestion> suggestionSet(Map<String, Suggestion> suggestionMap) {
        // todo: do this restriction only on a flag = strict, but for now we are doing for all
        boolean hasExactMatch = false;
        boolean hasEdgeGramMatch = false;
        boolean hasPhoneticMatch = false;
        for (Suggestion suggestion : suggestionMap.values()) {
            if (suggestion.getMatchLevel() == EdgeGram) {
                hasEdgeGramMatch = true;
            } else if (suggestion.getMatchLevel() == MatchLevel.Exact) {
                hasExactMatch = true;
            } else if (suggestion.getMatchLevel() == MatchLevel.Phonetic) {
                hasPhoneticMatch = true;
            }
        }

        SortedSet<Suggestion> suggestions = new TreeSet<>();
        for (Suggestion suggestion : suggestionMap.values()) {
            if ((hasExactMatch || hasEdgeGramMatch) && suggestion.getMatchLevel().getLevel() > MatchLevel.EdgeGram.getLevel()) {
                continue;
            }

            if (hasPhoneticMatch && suggestion.getMatchLevel().getLevel() > MatchLevel.Phonetic.getLevel()) {
                continue;
            }

            suggestions.add(suggestion);
        }

        return suggestions;
    }

    private void addSuggestion(Suggestion suggestion, Map<String, Suggestion> suggestionMap) {
        String key = suggestion.getSuggestion();
        if (suggestionMap.containsKey(key)) {
            Suggestion existingSuggestion = suggestionMap.get(key);
            int ret = suggestion.compareTo(existingSuggestion);
            if (ret >= 0) {
                // logger.debug("Not adding suggestion {} as better {}", suggestion, existingSuggestion);
                return;
            }
        }

        suggestionMap.put(key, suggestion);
    }

    public Map<String, SuggestionSet> fetchSuggestions(Client client, Collection<Conjunct> conjuncts, String... indices) {
        SuggestionBuilderTask task = new SuggestionBuilderTask(client, indices);

        try {
            return task.fetchSuggestions(conjuncts);
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Error in fetching suggestions for conjuncts: {}, indices: {}", e, conjuncts, indices);
        }

        return null;
    }

    class SuggestionBuilderTask {
        final Client client;
        final String[] indices;
        final String id;

        final PhoneticEncodingUtils tokenEncodingUtility = new PhoneticEncodingUtils();

        final List<String> queryKeys = new ArrayList<>();
        final Map<String, String> queryKeyToWordMap = new HashMap<>();
        final List<CompletableFuture<SuggestionSet>> suggestionResponses = new ArrayList<>();

        public SuggestionBuilderTask(Client client, String... indices) {
            this.client = client;
            this.indices = indices;
            id = StringUtils.join(this.indices, "/");
        }

        private CompletableFuture<SuggestionSet> store(String key, String word, CompletableFuture<SuggestionSet> future) {
            queryKeys.add(key);
            queryKeyToWordMap.put(key, word);
            suggestionResponses.add(future);

            return future;
        }

        private CompletableFuture<SuggestionSet> getOrBuildFuture(String key, String word, Function<Boolean, QueryBuilder> builder) throws ExecutionException {
            boolean number = NumberUtils.isNumber(word);

            // we do not store these... as re-calculating them would not be that costly
            if (number) {
                return store(key, word, NumberSuggestion);
            }

            boolean stopWord = StopWords.contains(word);

            if (word.length() == 1) {
                return store(key, word, SingleLetterSuggestion);
            }

            CompletableFuture<SuggestionSet> future = CachedCompletableResponses.get(key(key), () -> future(key, word, stopWord, builder.apply(stopWord)));

            return store(key, word, future);
        }

        private CompletableFuture<SuggestionSet> future(String key, String word, boolean stopWord, QueryBuilder queryBuilder) {
            if (logger.isDebugEnabled()) {
                logger.debug("Building future for key: {}, word: {} indices: {}", key, word, indices);
            }

            return CompletableFuture
                    .supplyAsync(() -> client.prepareSearch(indices).setSize(25).setQuery(queryBuilder).execute().actionGet(500, TimeUnit.MILLISECONDS), futureExecutorService)
                    .exceptionally(error -> {
                        logger.error("Error in executing future for key: {}, word: {} indices: {}", error, key, word, indices);
                        return null;
                    })
                    .thenApply(searchResponse -> {
                        boolean ignorePrefixSuggestions = key.endsWith("/compoundUni") || key.endsWith("/compoundBi");

                        Set<Suggestion> suggestions = wordSuggestions(word, searchResponse, ignorePrefixSuggestions);

                        return new SuggestionSet(false, stopWord, suggestions == null ? null : suggestions.toArray(new Suggestion[suggestions.size()]));
                    });
        }

        private void unigramWordMatch(Conjunct conjunct) throws ExecutionException {
            String key = conjunct.getKey();
            String word = conjunct.getTokens().get(0);

            getOrBuildFuture(key, word, (stopWord) -> {
                Set<String> encodings = encodings(word, stopWord);

                if (logger.isDebugEnabled()) {
                    logger.debug("Encoding for word {} = {}", word, encodings);
                }

                return new BoolQueryBuilder()
                        .should(buildWordQuery(Constants.UNIGRAM_DID_YOU_MEAN_INDEX_TYPE, "encodings", word, encodings))
                        .should(buildWordQuery(Constants.BIGRAM_DID_YOU_MEAN_INDEX_TYPE, "encodings", word, encodings))
                        .minimumNumberShouldMatch(1);

            });
        }

        @SuppressWarnings("unchecked")
        private void compoundWordMatch(Conjunct conjunct) throws ExecutionException {
            String word = StringUtils.join(conjunct.getTokens(), "");
            String key = conjunct.getKey();

            getOrBuildFuture(key, word, (stopWord) -> {
                Set<String> encodings = encodings(word, stopWord);

                if (logger.isDebugEnabled()) {
                    logger.debug("Encoding for word {} = {}", word, encodings);
                }

                QueryBuilder shingleQueryBuilder = null;
                if (conjunct.getLength() == 2) {
                    // we can form bigram query for these
                    String word1 = conjunct.getTokens().get(0);
                    String word2 = conjunct.getTokens().get(1);

                    Set<String> word1Encodings = encodings(word1, stopWord);
                    Set<String> word2Encodings = encodings(word2, stopWord);

                    if (logger.isDebugEnabled()) {
                        logger.debug("Encoding for word 1 {} = {}", word1, word1Encodings);
                        logger.debug("Encoding for word 2 {} = {}", word2, word2Encodings);
                    }

                    shingleQueryBuilder = QueryBuilders.boolQuery()
                            .must(buildWordQuery(Constants.BIGRAM_DID_YOU_MEAN_INDEX_TYPE, "word1Encodings", word1, word1Encodings))
                            .must(buildWordQuery(Constants.BIGRAM_DID_YOU_MEAN_INDEX_TYPE, "word2Encodings", word2, word2Encodings));
                }

                BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder()
                        .should(buildWordQuery(Constants.UNIGRAM_DID_YOU_MEAN_INDEX_TYPE, "encodings", word, encodings))
                        .should(buildWordQuery(Constants.BIGRAM_DID_YOU_MEAN_INDEX_TYPE, "encodings", word, encodings))
                        .minimumNumberShouldMatch(1);

                if (shingleQueryBuilder != null) {
                    boolQueryBuilder.should(shingleQueryBuilder);
                }

                return boolQueryBuilder;
            });
        }

        private String key(String key) {
            return id + "/" + key;
        }

        public Map<String, SuggestionSet> fetchSuggestions(Collection<Conjunct> conjuncts) throws ExecutionException, InterruptedException {
            if (conjuncts == null || conjuncts.size() == 0) {
                return null;
            }

            long startTime = System.currentTimeMillis();

            for (Conjunct conjunct : conjuncts) {
                if (conjunct.getLength() == 1) {
                    unigramWordMatch(conjunct);
                } else {
                    compoundWordMatch(conjunct);
                }
            }

            CompletableFuture<List<SuggestionSet>> allResponses = sequence(suggestionResponses);

            if (logger.isDebugEnabled()) {
                logger.debug("For conjuncts: {} and indices: {} build completable search responses: {} in {}ms", conjuncts, indices, suggestionResponses, (System.currentTimeMillis() - startTime));
            }

            startTime = System.currentTimeMillis();

            Map<String, SuggestionSet> suggestionsMap = allResponses.thenApply(responses -> {
                Map<String, SuggestionSet> map = new HashMap<>();

                int index = 0;
                for (SuggestionSet suggestions : responses) {
                    map.put(queryKeys.get(index), suggestions);
                    index++;
                }

                return map;
            }).get();

            if (logger.isDebugEnabled()) {
                logger.debug("For conjuncts: {} and indices: {} created suggestions: {} in {}ms", conjuncts, indices, suggestionsMap, (System.currentTimeMillis() - startTime));
            }

            return suggestionsMap;
        }
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
}

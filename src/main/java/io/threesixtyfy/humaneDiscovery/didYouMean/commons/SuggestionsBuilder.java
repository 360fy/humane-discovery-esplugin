package io.threesixtyfy.humaneDiscovery.didYouMean.commons;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.commons.lang3.StringUtils;
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
import java.util.Collection;
import java.util.HashMap;
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
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.join;

public class SuggestionsBuilder {

    private final ESLogger logger = Loggers.getLogger(SuggestionsBuilder.class);

    private static final SuggestionsBuilder instance = new SuggestionsBuilder();

    private final Map<Character, Character> similarCharacterMap = new HashMap<>();

    private final LevensteinDistance levensteinDistance = new LevensteinDistance();

    private final Cache<String, CompletableFuture<Set<Suggestion>>> CachedCompletableResponses = CacheBuilder
            .newBuilder()
            .maximumSize(200)
            .build();

    private final Cache<String, Set<String>> CachedEncodings = CacheBuilder
            .newBuilder()
            .maximumSize(200)
            .build();

    private final ExecutorService futureExecutorService = new ThreadPoolExecutor(5, 5, 30, TimeUnit.SECONDS, new LinkedBlockingQueue<>(100));

    private SuggestionsBuilder() {
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

    private Set<String> encodings(PhoneticEncodingUtils tokenEncodingUtility, String token) throws ExecutionException {
        return CachedEncodings.get(token, () -> {
            logger.info("Building encoding for token: {}", token);

            return tokenEncodingUtility.buildEncodings(token);
        });
    }

    private String bigram(String word1, String word2) {
        return word1 + word2;
    }


    private BoolQueryBuilder buildWordQuery(String type, String field, String word, Set<String> phoneticEncodings) {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery()
                .filter(QueryBuilders.typeQuery(type))
                .should(QueryBuilders.termQuery(field, word).boost(100.0f));

        phoneticEncodings.stream().forEach(w -> {

            TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery(field, w);

            boolQueryBuilder.should(termQueryBuilder);
        });

        int clauses = 1 + phoneticEncodings.size();
        if (clauses > 2) {
            boolQueryBuilder.minimumNumberShouldMatch(2);
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

    @SuppressWarnings("unchecked")
    private void buildSuggestion(Map<String, Object> source, String inputWord, String suggestedWord, int inputWordLength, Map<String, Suggestion> suggestionMap) {
        int totalCount = (int) source.get("totalCount");
        int countAsFullWord = (int) source.get("countAsFullWord");

        int distance = 0;
        int similarity = inputWordLength;
        boolean edgeGram = (countAsFullWord * 100.0 / totalCount) < 40.0;
        MatchLevel matchLevel = MatchLevel.Exact;

        if (!inputWord.equals(suggestedWord)) {
            // we have exact match here
            // we select the word with proper edit distance
            distance = StringUtils.getLevenshteinDistance(suggestedWord, inputWord);
            similarity = getFuzzyDistance(suggestedWord, inputWord, Locale.ENGLISH);

            matchLevel = MatchLevel.Phonetic;
        }

        float similarityPercentage = Math.round(similarity * 10.0f / inputWordLength);
        float editDistancePercentage = Math.round(distance * 10.0f / inputWordLength);
        double jwDistance = StringUtils.getJaroWinklerDistance(inputWord, suggestedWord);
        double lDistance = levensteinDistance.getDistance(inputWord, suggestedWord);

//        logger.info(">>>>>>>>> For input: {}, found suggestion: {}, originalWords: {} with similarity: {}, edit: {}, JWD:{}, LD:{}",
//                inputWord, suggestedWord, source.get("originalWords"),
//                similarityPercentage, editDistancePercentage, jwDistance, lDistance);

        // anything with more than 5.0 edit distance we ignore right away
        if (editDistancePercentage > 4.0 || similarity < 4.0 && jwDistance < 0.75 && lDistance < 0.75) {
            return;
        }

        if (edgeGram) {
            if (matchLevel == MatchLevel.Phonetic) {
                matchLevel = MatchLevel.EdgeGramPhonetic;
            } else {
                matchLevel = MatchLevel.EdgeGram;
            }
        } else {
            addSuggestion(new Suggestion(suggestedWord, suggestedWord, matchLevel, distance, editDistancePercentage, similarity, similarityPercentage, totalCount), suggestionMap);
        }

        List<Map<String, Object>> originalWordsInfoList = (List<Map<String, Object>>) source.get("originalWords");
        for (Map<String, Object> originalWordInfo : originalWordsInfoList) {
            String originalWord = (String) originalWordInfo.get("word");
            int originalWordCount = (int) originalWordInfo.get("totalCount");

            // we count similarity with edgeGram
            addSuggestion(new Suggestion(originalWord, suggestedWord, matchLevel, distance, editDistancePercentage, similarity, similarityPercentage, originalWordCount), suggestionMap);
        }
    }

    private Set<Suggestion> joinedWordSuggestions(String inputWord, SearchResponse searchResponse) {
        if (searchResponse == null || searchResponse.getHits() == null || searchResponse.getHits().getHits() == null) {
            return null;
        }

        int inputWordLength = inputWord.length();

        Map<String, Suggestion> suggestionMap = new HashMap<>();

        for (SearchHit searchHit : searchResponse.getHits().getHits()) {
            // check for exact suggestion
            Map<String, Object> source = searchHit.getSource();

            String suggestedWord1 = (String) source.get("word1");
            String suggestedWord2 = (String) source.get("word2");
            String suggestedWord = (String) source.get("word");

            if (inputWord.length() <= suggestedWord1.length() + Math.min(2, suggestedWord2.length() / 2)) {
                continue;
            }

            buildSuggestion(source, inputWord, suggestedWord, inputWordLength, suggestionMap);
        }

        if (suggestionMap.size() == 0) {
            return null;
        }

        return suggestionSet(suggestionMap);
    }

    @SuppressWarnings("unchecked")
    private Set<Suggestion> wordSuggestions(String inputWord, SearchResponse searchResponse) {
        if (searchResponse == null || searchResponse.getHits() == null || searchResponse.getHits().getHits() == null) {
            return null;
        }

        int inputWordLength = inputWord.length();

        Map<String, Suggestion> suggestionMap = new HashMap<>();

        for (SearchHit searchHit : searchResponse.getHits().getHits()) {
            // check for exact suggestion
            Map<String, Object> source = searchHit.getSource();

            String suggestedWord = (String) source.get("word");

            buildSuggestion(source, inputWord, suggestedWord, inputWordLength, suggestionMap);
        }

        if (suggestionMap.size() == 0) {
            return null;
        }

        return suggestionSet(suggestionMap);
    }

    private SortedSet<Suggestion> suggestionSet(Map<String, Suggestion> suggestionMap) {
        // todo: do this on a flag = strict, but for now we are doing for all
        boolean hasExactMatch = false;
        boolean hasEdgeGramMatch = false;
        for (Suggestion suggestion : suggestionMap.values()) {
            if (suggestion.getMatchLevel() == MatchLevel.EdgeGram) {
                hasEdgeGramMatch = true;
            } else if (suggestion.getMatchLevel() == MatchLevel.Exact) {
                hasExactMatch = true;
            }
        }

        SortedSet<Suggestion> suggestions = new TreeSet<>();
        for (Suggestion suggestion : suggestionMap.values()) {
            if (!hasExactMatch && !hasEdgeGramMatch || suggestion.getMatchLevel() == MatchLevel.EdgeGram || suggestion.getMatchLevel() == MatchLevel.Exact) {
                // logger.info("Adding suggestion: {}", suggestion);
                suggestions.add(suggestion);
            }
        }

        return suggestions;
    }

    private void addSuggestion(Suggestion suggestion, Map<String, Suggestion> suggestionMap) {
        String key = suggestion.getSuggestion();
        if (suggestionMap.containsKey(key)) {
            Suggestion existingSuggestion = suggestionMap.get(key);
            int ret = suggestion.compareTo(existingSuggestion);
            if (ret >= 0) {
                // logger.info("Not adding suggestion {} as better {}", suggestion, existingSuggestion);
                return;
            }
        }

        suggestionMap.put(key, suggestion);
    }

    public Map<String, Set<Suggestion>> fetchSuggestions(Client client, Collection<Conjunct> conjuncts, String... indices) {
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
        final List<CompletableFuture<Set<Suggestion>>> suggestionResponses = new ArrayList<>();

        public SuggestionBuilderTask(Client client, String... indices) {
            this.client = client;
            this.indices = indices;
            id = StringUtils.join(this.indices,"/");
        }

        private void store(String key, String word, CompletableFuture<Set<Suggestion>> future) {
            queryKeys.add(key);
            queryKeyToWordMap.put(key, word);
            suggestionResponses.add(future);
        }

        private CompletableFuture<Set<Suggestion>> future(String key, String word, QueryBuilder queryBuilder) {
            logger.info("Building future for key: {}, word: {} indices: {}", key, word, indices);

            return CompletableFuture
                    .supplyAsync(() -> client.prepareSearch(indices).setQuery(queryBuilder).execute().actionGet(500, TimeUnit.MILLISECONDS), futureExecutorService)
                    .exceptionally(error -> {
                        logger.error("Error in executing future for key: {}, word: {} indices: {}", error, key, word, indices);
                        return null;
                    })
                    .thenApply(searchResponse -> {
                        Set<Suggestion> suggestions;

                        if (key.endsWith("/joined")) {
                            suggestions = joinedWordSuggestions(word, searchResponse);
                        } else {
                            suggestions = wordSuggestions(word, searchResponse);
                        }

                        return suggestions;
                    });
        }

        private void unigramWordMatch(Conjunct conjunct) throws ExecutionException {
            String key = conjunct.getKey();
            String word = conjunct.getTokens().get(0);

            CompletableFuture<Set<Suggestion>> future = CachedCompletableResponses.get(key(key), () -> {
                Set<String> encodings = encodings(tokenEncodingUtility, word);

                QueryBuilder queryBuilder = buildWordQuery(Constants.UNIGRAM_DID_YOU_MEAN_INDEX_TYPE, "encodings", word, encodings);

                return future(key, word, queryBuilder);
            });

            store(key, word, future);
        }

        private void joinedWordMatch(Conjunct conjunct) throws ExecutionException {
            String key = conjunct.getKey() + "/joined";
            String word = conjunct.getTokens().get(0);

            CompletableFuture<Set<Suggestion>> future = CachedCompletableResponses.get(key(key), () -> {
                Set<String> encodings = encodings(tokenEncodingUtility, word);

                QueryBuilder queryBuilder = buildWordQuery(Constants.BIGRAM_DID_YOU_MEAN_INDEX_TYPE, "encodings", word, encodings);

                return future(key, word, queryBuilder);
            });

            store(key, word, future);
        }

        private void shingleWordMatch(Conjunct conjunct) throws ExecutionException {
            if (conjunct.getLength() == 2) {
                // we can form bigram query for these
                String word1 = conjunct.getTokens().get(0);
                String word2 = conjunct.getTokens().get(1);

                String key = conjunct.getKey() + "/shingle";
                String word = bigram(word1, word2);

                CompletableFuture<Set<Suggestion>> future = CachedCompletableResponses.get(key(key), () -> {
                    Set<String> word1Encodings = encodings(tokenEncodingUtility, word1);
                    Set<String> word2Encodings = encodings(tokenEncodingUtility, word2);

                    QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                            .must(buildWordQuery(Constants.BIGRAM_DID_YOU_MEAN_INDEX_TYPE, "word1Encodings", word1, word1Encodings))
                            .must(buildWordQuery(Constants.BIGRAM_DID_YOU_MEAN_INDEX_TYPE, "word2Encodings", word2, word2Encodings));

                    return future(key, word, queryBuilder);
                });

                store(key, word, future);
            }
        }

        @SuppressWarnings("unchecked")
        private void compoundWordMatch(Conjunct conjunct) throws ExecutionException {
            String key = conjunct.getKey() + "/compound";
            String word = join(conjunct.getTokens());

            CompletableFuture<Set<Suggestion>> future = CachedCompletableResponses.get(key(key), () -> {
                Set<String> encodings = encodings(tokenEncodingUtility, word);

                QueryBuilder queryBuilder = buildWordQuery(Constants.UNIGRAM_DID_YOU_MEAN_INDEX_TYPE, "encodings", word, encodings);

                return future(key, word, queryBuilder);
            });

            store(key, word, future);
        }

        private String key(String key) {
            return id + "/" + key;
        }

        public Map<String, Set<Suggestion>> fetchSuggestions(Collection<Conjunct> conjuncts) throws ExecutionException, InterruptedException {
            if (conjuncts == null || conjuncts.size() == 0) {
                return null;
            }

            long startTime = System.currentTimeMillis();

            for (Conjunct conjunct : conjuncts) {
                if (conjunct.getLength() == 1) {
                    unigramWordMatch(conjunct);
                    joinedWordMatch(conjunct);
                } else {
                    shingleWordMatch(conjunct);
                    compoundWordMatch(conjunct);
                }
            }

            CompletableFuture<List<Set<Suggestion>>> allResponses = sequence(suggestionResponses);

            logger.info("For conjuncts: {} and indices: {} build completable search responses: {} in {}ms", conjuncts, indices, suggestionResponses, (System.currentTimeMillis() - startTime));

            startTime = System.currentTimeMillis();

            Map<String, Set<Suggestion>> suggestionsMap = allResponses.thenApply(responses -> {
                Map<String, Set<Suggestion>> map = new HashMap<>();

                int index = 0;
                for (Set<Suggestion> suggestions : responses) {
                    map.put(queryKeys.get(index), suggestions);
                    index++;
                }

                return map;
            }).get();

            logger.info("For conjuncts: {} and indices: {} created suggestions: {} in {}ms", conjuncts, indices, suggestionsMap, (System.currentTimeMillis() - startTime));

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

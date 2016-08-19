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
import org.elasticsearch.search.SearchHitField;

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
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.threesixtyfy.humaneDiscovery.didYouMean.commons.MatchLevel.EdgeGram;
import static io.threesixtyfy.humaneDiscovery.didYouMean.commons.MatchLevel.EdgeGramPhonetic;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;

public class SuggestionsBuilder {

    public static final String HUMANE_QUERY_ANALYZER = "humane_query_analyzer";
    public static final String DUMMY_FIELD = "dummyField";
    public static final float GRAM_START_BOOST = 5.0f;
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
    public static final String[] FETCH_SOURCES = new String[]{FIELD_ENCODINGS, FIELD_TOTAL_WEIGHT, FIELD_TOTAL_COUNT, FIELD_COUNT_AS_FULL_WORD, FIELD_WORD, FIELD_WORD_1, FIELD_WORD_2, "originalWords.*"};
    public static final String FIELD_WORD_1_ENCODINGS = "word1Encodings";
    public static final String FIELD_WORD_2_ENCODINGS = "word2Encodings";

    private final ESLogger logger = Loggers.getLogger(SuggestionsBuilder.class);

    private static final SuggestionsBuilder instance = new SuggestionsBuilder();

    private final Map<Character, Character> similarCharacterMap = new HashMap<>();

    private final LevensteinDistance levensteinDistance = new LevensteinDistance();

    private final Map<String, String[]> synonyms = new HashMap<>();

    private final CompletableFuture<SuggestionSet> NumberSuggestion = new CompletableFuture<>();

    private final CompletableFuture<SuggestionSet> SingleLetterSuggestion = new CompletableFuture<>();

    // TODO: expire on event only
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

    private final FastObjectPool<PhoneticEncodingUtils> phoneticEncodingUtilsPool;

    private SuggestionsBuilder() {
        NumberSuggestion.complete(new SuggestionSet(true, false, null));

        SingleLetterSuggestion.complete(new SuggestionSet(false, false, new Suggestion[0]));

        similarCharacterMap.put('j', 'z');
        similarCharacterMap.put('z', 'j');

        phoneticEncodingUtilsPool = new FastObjectPool<>(new PhoneticEncodingUtils.Factory(), 20);

        synonyms.put("tablet", new String[]{"tablets"});
        synonyms.put("tablets", new String[]{"tablet"});
        synonyms.put("injection", new String[]{"injections"});
        synonyms.put("injections", new String[]{"injection"});
        synonyms.put("advance", new String[]{"advanced"});
        synonyms.put("advanced", new String[]{"advance"});
        synonyms.put("capsule", new String[]{"capsules"});
        synonyms.put("capsules", new String[]{"capsule"});
        synonyms.put("hilamya", new String[]{"himalaya"});
    }

    public static SuggestionsBuilder INSTANCE() {
        return instance;
    }

    public List<String> tokens(AnalysisService analysisService, String query) throws IOException {
        Analyzer analyzer = analysisService.analyzer(HUMANE_QUERY_ANALYZER);
        if (analyzer == null) {
            throw new RuntimeException("No humane_query_analyzer found");
        }

        TokenStream tokenStream = analyzer.tokenStream(DUMMY_FIELD, query);

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

                FastObjectPool.Holder<PhoneticEncodingUtils> poolHolder = null;
                try {
                    poolHolder = phoneticEncodingUtilsPool.take();
                    if (poolHolder != null && poolHolder.getValue() != null) {
                        return poolHolder.getValue().buildEncodings(token, stopWord);
                    } else {
                        return null;
                    }
                } finally {
                    if (poolHolder != null) {
                        try {
                            phoneticEncodingUtilsPool.release(poolHolder);
                        } catch (Exception ex) {
                            // ignore it
                        }
                    }
                }
            });
        } catch (ExecutionException e) {
            return null;
        }
    }

    private BoolQueryBuilder buildWordQuery(String type, String field, String word, Set<String> phoneticEncodings) {
        BoolQueryBuilder boolQueryBuilder = boolQuery()
                .filter(QueryBuilders.typeQuery(type))
                .should(QueryBuilders.termQuery(field, word).boost(EXACT_TERM_BOOST));

        phoneticEncodings.stream().forEach(w -> {

            TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery(field, w);

            if (w.startsWith(Constants.GRAM_START_PREFIX)) {
                termQueryBuilder.boost(GRAM_START_BOOST);
            }

            if (w.startsWith(Constants.GRAM_END_PREFIX)) {
                termQueryBuilder.boost(GRAM_END_BOOST);
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

                    // subsequent character matches further improve the score.
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
                                 SearchHit searchHit,
                                 String inputWord,
                                 Suggestion.TokenType tokenType,
                                 String suggestedWord,
                                 String display,
                                 int inputWordLength,
                                 boolean ignorePrefixSuggestions,
                                 Map<String, Suggestion> suggestionMap) {
        double totalWeight = fieldValue(searchHit, FIELD_TOTAL_WEIGHT);
        int totalCount = fieldValue(searchHit, FIELD_TOTAL_COUNT);
        int countAsFullWord = fieldValue(searchHit, FIELD_COUNT_AS_FULL_WORD);
        List<Object> suggestedWordEncodings = fieldValues(searchHit, FIELD_ENCODINGS);

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
                for (Object e : suggestedWordEncodings) {
                    String encoding = (String) e;
                    if (e != null && !encoding.startsWith(Constants.GRAM_PREFIX) && !encoding.startsWith(Constants.GRAM_START_PREFIX) && !encoding.startsWith(Constants.GRAM_END_PREFIX) && inputWordEncodings.contains(e)) {
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

            // more than 40% drop in lDistance -- earlier 25%
            if ((bestStats.lDistance - currentStats.lDistance) / currentStats.lDistance > 0.40) {
                return;
            }

            // more than 40% drop in jwDistance -- earlier 25%
            if ((bestStats.jwDistance - currentStats.jwDistance) / currentStats.jwDistance > 0.40) {
                return;
            }

            // more than 5 times drop from best score -- earlier 3 times
            if (bestStats.score / currentStats.score > 5.0) {
                return;
            }

            // more than 3.0 drop from previous score -- earlier 2 times
            if (previousStats != null && bestStats.score != previousStats.score && previousStats.score / currentStats.score > 3.0) {
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

        List<Object> originalWordsInfoList = fieldValues(searchHit, FIELD_ORIGINAL_WORDS); //searchHit.sourceAsMap().get("originalWords");

        for (Object originalWordData : originalWordsInfoList) {
            Map<String, Object> originalWordInfo = (Map<String, Object>) originalWordData;
            String originalWord = (String) originalWordInfo.get(FIELD_WORD);
            String originalDisplay = (String) originalWordInfo.get(FIELD_DISPLAY);
            int originalWordCount = (int) originalWordInfo.get(FIELD_TOTAL_COUNT);
            double originalWordWeight = (double) originalWordInfo.get(FIELD_TOTAL_WEIGHT);

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

    public static long minimum(long... values) {
        int len = values.length;
        long current = values[0];

        for (int i = 1; i < len; i++) {
            current = Math.min(values[i], current);
        }

        return current;
    }

    public static int damerauLevenshteinDistance(CharSequence source, CharSequence target) {
        if (source == null || "".equals(source)) {
            return target == null || "".equals(target) ? 0 : target.length();
        } else if (target == null || "".equals(target)) {
            return source.length();
        }

        int srcLen = source.length();
        int targetLen = target.length();
        int[][] distanceMatrix = new int[srcLen + 1][targetLen + 1];

        // We need indexers from 0 to the length of the source string.
        // This sequential set of numbers will be the row "headers"
        // in the matrix.
        for (int srcIndex = 0; srcIndex <= srcLen; srcIndex++) {
            distanceMatrix[srcIndex][0] = srcIndex;
        }

        // We need indexers from 0 to the length of the target string.
        // This sequential set of numbers will be the
        // column "headers" in the matrix.
        for (int targetIndex = 0; targetIndex <= targetLen; targetIndex++) {
            // Set the value of the first cell in the column
            // equivalent to the current value of the iterator
            distanceMatrix[0][targetIndex] = targetIndex;
        }

        for (int srcIndex = 1; srcIndex <= srcLen; srcIndex++) {
            for (int targetIndex = 1; targetIndex <= targetLen; targetIndex++) {
                // If the current characters in both strings are equal
                int cost = source.charAt(srcIndex - 1) == target.charAt(targetIndex - 1) ? 0 : 1;

                // Find the current distance by determining the shortest path to a
                // match (hence the 'minimum' calculation on distances).
                distanceMatrix[srcIndex][targetIndex] = (int) minimum(
                        // Character match between current character in
                        // source string and next character in target
                        distanceMatrix[srcIndex - 1][targetIndex] + 1,
                        // Character match between next character in
                        // source string and current character in target
                        distanceMatrix[srcIndex][targetIndex - 1] + 1,
                        // No match, at current, add cumulative penalty
                        distanceMatrix[srcIndex - 1][targetIndex - 1] + cost);

                // We don't want to do the next series of calculations on
                // the first pass because we would get an index out of bounds
                // exception.
                if (srcIndex == 1 || targetIndex == 1) {
                    continue;
                }

                // transposition check (if the current and previous
                // character are switched around (e.g.: t[se]t and t[es]t)...
                if (source.charAt(srcIndex - 1) == target.charAt(targetIndex - 2) && source.charAt(srcIndex - 2) == target.charAt(targetIndex - 1)) {
                    // What's the minimum cost between the current distance
                    // and a transposition.
                    distanceMatrix[srcIndex][targetIndex] = (int) minimum(
                            // Current cost
                            distanceMatrix[srcIndex][targetIndex],
                            // Transposition
                            distanceMatrix[srcIndex - 2][targetIndex - 2] + cost);
                }
            }
        }

        return distanceMatrix[srcLen][targetLen];
    }

    private CandidateStats buildCandidateStats(String inputWord, String suggestedWord, float score) {
        int distance = 0;
        int similarity = 0;

        if (!inputWord.equals(suggestedWord)) {
            // we have exact match here
            // we select the word with proper edit distance
            distance = damerauLevenshteinDistance(inputWord, suggestedWord);
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
    private <V> V fieldValue(SearchHit searchHit, String field) {
        SearchHitField searchHitField = searchHit.field(field);
        if (searchHitField != null) {
            return searchHitField.value();
        } else {
            return (V) searchHit.getSource().get(field);
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

            String type = searchHit.type();

            String suggestedWord = fieldValue(searchHit, FIELD_WORD);

            String displayWord;

            Suggestion.TokenType tokenType;

            if (Constants.UNIGRAM_DID_YOU_MEAN_INDEX_TYPE.equals(type)) {
                displayWord = suggestedWord;
                tokenType = Suggestion.TokenType.Uni;
            } else {
                String suggestedWord1 = fieldValue(searchHit, FIELD_WORD_1);
                String suggestedWord2 = fieldValue(searchHit, FIELD_WORD_2);
                displayWord = suggestedWord1 + " " + suggestedWord2;
                tokenType = Suggestion.TokenType.Bi;

                if (inputWord.length() <= suggestedWord1.length() + Math.min(2, suggestedWord2.length() / 2)) {
                    continue;
                }
            }

            buildSuggestion(bestCandidateStats,
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

    private CandidateStats[] buildAllCandidateStats(String inputWord, SearchResponse searchResponse, int hitsCount) {
        CandidateStats[] candidateStats = new CandidateStats[hitsCount];

        for (int i = 0; i < hitsCount; i++) {
            SearchHit searchHit = searchResponse.getHits().getHits()[i];

            // check for exact suggestion
            candidateStats[i] = buildCandidateStats(inputWord, fieldValue(searchHit, FIELD_WORD), searchHit.score());
        }

        return candidateStats;
    }

    private SortedSet<Suggestion> suggestionSet(String word, Map<String, Suggestion> suggestionMap) {
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

        boolean hasSynonyms = false;
        String[] synonymList = synonyms.get(word);
        if (synonymList != null) {

            for (String synonym : synonymList) {
                // TODO: have special type to capture synonyms
                suggestions.add(new Suggestion(Suggestion.TokenType.Uni, synonym, synonym, synonym, MatchLevel.Synonym, 0, 0, 0, 0, 0, 0, 0));
                hasSynonyms = true;
            }
        }

        for (Suggestion suggestion : suggestionMap.values()) {
            if ((hasExactMatch || hasEdgeGramMatch) && suggestion.getMatchLevel().getLevel() > MatchLevel.EdgeGram.getLevel()) {
                continue;
            }

            if (hasSynonyms || hasPhoneticMatch && suggestion.getMatchLevel().getLevel() > MatchLevel.Phonetic.getLevel()) {
                continue;
            }

            suggestions.add(suggestion);
        }

        if (suggestions.size() > 0 && suggestions.first().getMatchLevel() == EdgeGramPhonetic && suggestions.first().getTokenType() == Suggestion.TokenType.Uni) {
            for (Suggestion suggestion : suggestions) {
                if (suggestion.getTokenType() == Suggestion.TokenType.Bi) {
                    suggestion.setIgnore(true);
                }
            }
        }

        if (logger.isDebugEnabled()) {
            logger.debug(">>> Final Suggestions: {}", suggestions);
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

        final List<String> queryKeys = new ArrayList<>();
        final List<CompletableFuture<SuggestionSet>> suggestionResponses = new ArrayList<>();

        public SuggestionBuilderTask(Client client, String... indices) {
            this.client = client;
            this.indices = indices;
            id = StringUtils.join(this.indices, "/");
        }

        private CompletableFuture<SuggestionSet> store(String key, String word, CompletableFuture<SuggestionSet> future) {
            queryKeys.add(key);
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
                    .supplyAsync(() ->
                                    client.prepareSearch(indices)
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
                        .should(buildWordQuery(Constants.UNIGRAM_DID_YOU_MEAN_INDEX_TYPE, FIELD_ENCODINGS, word, encodings))
                        .should(buildWordQuery(Constants.BIGRAM_DID_YOU_MEAN_INDEX_TYPE, FIELD_ENCODINGS, word, encodings))
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
                            .must(buildWordQuery(Constants.BIGRAM_DID_YOU_MEAN_INDEX_TYPE, FIELD_WORD_1_ENCODINGS, word1, word1Encodings))
                            .must(buildWordQuery(Constants.BIGRAM_DID_YOU_MEAN_INDEX_TYPE, FIELD_WORD_2_ENCODINGS, word2, word2Encodings));
                }

                BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder()
                        .should(buildWordQuery(Constants.UNIGRAM_DID_YOU_MEAN_INDEX_TYPE, FIELD_ENCODINGS, word, encodings))
                        .should(buildWordQuery(Constants.BIGRAM_DID_YOU_MEAN_INDEX_TYPE, FIELD_ENCODINGS, word, encodings))
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

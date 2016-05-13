package io.threesixtyfy.humaneDiscovery.didYouMean.commons;

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.search.spell.LevensteinDistance;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

public class SuggestionsBuilder extends AbstractComponent {

    private final Map<Character, Character> similarCharacterMap = new HashMap<>();

    private final LevensteinDistance levensteinDistance = new LevensteinDistance();

    private final Client client;

    @Inject
    public SuggestionsBuilder(Client client, Settings settings) {
        super(settings);

        this.client = client;

        similarCharacterMap.put('j', 'z');
        similarCharacterMap.put('z', 'j');
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

    // TODO: cache token to encodings
    private Set<String> encodings(PhoneticEncodingUtils tokenEncodingUtility, String token, Map<String, Set<String>> encodingCache) {
        if (!encodingCache.containsKey(token)) {
            Set<String> encodings = tokenEncodingUtility.buildEncodings(token);
            encodingCache.put(token, encodings);
        }

        return encodingCache.get(token);
    }

    private String bigram(String word1, String word2) {
        return word1 + word2;
    }

    @SuppressWarnings("unchecked")
    public Map<String, Set<Suggestion>> fetchSuggestions(Collection<Conjunct> conjuncts, String... indices) {
        PhoneticEncodingUtils tokenEncodingUtility = new PhoneticEncodingUtils();

        Map<String, Set<String>> encodingCache = new HashMap<>();

        List<SearchRequestBuilder> requestBuilders = new LinkedList<>();

        List<String> queryKeys = new ArrayList<>();
        Map<String, String> queryKeyToWordMap = new HashMap<>();

        for (Conjunct conjunct : conjuncts) {
            if (conjunct.getLength() == 1) {
                String key = conjunct.getKey();
                String word = conjunct.getTokens().get(0);
                Set<String> encodings = encodings(tokenEncodingUtility, word, encodingCache);
                requestBuilders.add(client.prepareSearch(indices)
                        .setQuery(buildWordQuery(Constants.UNIGRAM_DID_YOU_MEAN_INDEX_TYPE, "encodings", word, encodings)));

                queryKeys.add(key);
                queryKeyToWordMap.put(key, word);

                requestBuilders.add(client.prepareSearch(indices)
                        .setQuery(buildWordQuery(Constants.BIGRAM_DID_YOU_MEAN_INDEX_TYPE, "encodings", word, encodings)));

                key = key + "/joined";
                queryKeys.add(key);
                queryKeyToWordMap.put(key, word);
            } else {
                if (conjunct.getLength() == 2) {
                    // we can form bigram query for these
                    String word1 = conjunct.getTokens().get(0);
                    String word2 = conjunct.getTokens().get(1);

                    Set<String> word1Encodings = encodings(tokenEncodingUtility, word1, encodingCache);
                    Set<String> word2Encodings = encodings(tokenEncodingUtility, word2, encodingCache);

                    requestBuilders.add(client.prepareSearch(indices)
                            .setQuery(QueryBuilders.boolQuery()
                                    .must(buildWordQuery(Constants.BIGRAM_DID_YOU_MEAN_INDEX_TYPE, "word1Encodings", word1, word1Encodings))
                                    .must(buildWordQuery(Constants.BIGRAM_DID_YOU_MEAN_INDEX_TYPE, "word2Encodings", word2, word2Encodings))));

                    String key = conjunct.getKey() + "/shingle";
                    queryKeys.add(key);
                    queryKeyToWordMap.put(key, bigram(word1, word2));
                }

                String word = StringUtils.join(conjunct.getTokens());
                Set<String> encodings = encodings(tokenEncodingUtility, word, encodingCache);
                requestBuilders.add(client.prepareSearch(indices)
                        .setQuery(buildWordQuery(Constants.UNIGRAM_DID_YOU_MEAN_INDEX_TYPE, "encodings", word, encodings)));

                String key = conjunct.getKey() + "/compound";
                queryKeys.add(key);
                queryKeyToWordMap.put(key, word);
            }
        }

        int requestSize = requestBuilders.size();

        ActionRequestBuilder searchRequestBuilder;
        if (requestSize == 1) {
            // we fire only one request
            String queryKey = queryKeys.get(0);
            searchRequestBuilder = requestBuilders.get(0);
            SearchResponse searchResponse = (SearchResponse) searchRequestBuilder.execute().actionGet();
            return suggestionsMap(queryKey, queryKeyToWordMap.get(queryKey), searchResponse);
        } else {
            MultiSearchRequestBuilder multiSearchRequestBuilder = client.prepareMultiSearch();
            requestBuilders.forEach(multiSearchRequestBuilder::add);

            searchRequestBuilder = multiSearchRequestBuilder;
            MultiSearchResponse multiSearchResponse = (MultiSearchResponse) searchRequestBuilder.execute().actionGet();
            return suggestionsMap(queryKeys, queryKeyToWordMap, multiSearchResponse);
        }
    }

    private BoolQueryBuilder buildWordQuery(String type, String field, String word, Set<String> phoneticEncodings) {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery().minimumNumberShouldMatch(2)
                .filter(QueryBuilders.typeQuery(type))
                .should(QueryBuilders.termQuery(field, word).boost(100.0f));

        phoneticEncodings.stream().forEach(w -> {

            TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery(field, w);

            boolQueryBuilder.should(termQueryBuilder);
        });

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

    private Map<String, Set<Suggestion>> suggestionsMap(List<String> queryKeys, Map<String, String> queryKeyToWordMap, MultiSearchResponse multiSearchResponse) {
        Map<String, Set<Suggestion>> suggestionsMap = new HashMap<>();

        int index = 0;
        for (String queryKey : queryKeys) {
            SearchResponse searchResponse = multiSearchResponse.getResponses()[index].getResponse();

            Set<Suggestion> suggestions;

            if (queryKey.endsWith("/shingle")) {
                suggestions = bigramSuggestions(queryKeyToWordMap.get(queryKey), searchResponse);
            } else {
                suggestions = unigramSuggestions(queryKeyToWordMap.get(queryKey), searchResponse);
            }

            if (suggestions != null) {
                suggestionsMap.put(queryKey, suggestions);
            }

            index++;
        }

        return suggestionsMap;
    }

    private Map<String, Set<Suggestion>> suggestionsMap(String queryKey, String inputWord, SearchResponse searchResponse) {
        Map<String, Set<Suggestion>> suggestionsMap = new HashMap<>();

        Set<Suggestion> suggestions = unigramSuggestions(inputWord, searchResponse);

        if (suggestions != null) {
            suggestionsMap.put(queryKey, suggestions);
        }

        return suggestionsMap;
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

    @SuppressWarnings("unchecked")
    private Set<Suggestion> bigramSuggestions(String inputWord, SearchResponse searchResponse) {
        int inputWordLength = inputWord.length();

        Map<String, Suggestion> suggestionMap = new HashMap<>();

        for (SearchHit searchHit : searchResponse.getHits().getHits()) {
            // check for exact suggestion
            Map<String, Object> source = searchHit.getSource();

            String suggestedWord1 = (String) source.get("word1");
            String suggestedWord2 = (String) source.get("word2");
            String suggestedWord = bigram(suggestedWord1, suggestedWord2);

            buildSuggestion(source, inputWord, suggestedWord, inputWordLength, suggestionMap);
        }

        if (suggestionMap.size() == 0) {
            return null;
        }

        SortedSet<Suggestion> suggestions = new TreeSet<>();
        suggestions.addAll(suggestionMap.values());
        return suggestions;
    }

    @SuppressWarnings("unchecked")
    private Set<Suggestion> unigramSuggestions(String inputWord, SearchResponse searchResponse) {
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

        SortedSet<Suggestion> suggestions = new TreeSet<>();
        suggestions.addAll(suggestionMap.values());
        return suggestions;
    }

    private void addSuggestion(Suggestion suggestion, Map<String, Suggestion> suggestionMap) {
        String key = suggestion.getSuggestion();
        if (suggestionMap.containsKey(key)) {
            Suggestion existingSuggestion = suggestionMap.get(key);
            int ret = suggestion.compareTo(existingSuggestion);
            if (ret >= 0) {
//                logger.info("Not adding suggestion {} as better {}", suggestion, existingSuggestion);
                return;
            }
        }

        suggestionMap.put(key, suggestion);
    }
}

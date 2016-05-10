package io.threesixtyfy.humaneDiscovery.didYouMean.action;

import io.threesixtyfy.humaneDiscovery.commons.TokenEncodingUtility;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class TransportDidYouMeanAction extends HandledTransportAction<DidYouMeanRequest, DidYouMeanResponse> {

    public static final String UNIGRAM_DID_YOU_MEAN_INDEX_TYPE = "didYouMean";

    public static final String BIGRAM_DID_YOU_MEAN_INDEX_TYPE = "didYouMeanBigram";

    private final ClusterService clusterService;

    private final Client client;

    private final IndicesService indicesService;

    @Inject
    public TransportDidYouMeanAction(Settings settings, String actionName, ThreadPool threadPool,
                                     TransportService transportService, ClusterService clusterService, IndicesService indicesService,
                                     Client client, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, actionName, threadPool, transportService, actionFilters, indexNameExpressionResolver, DidYouMeanRequest.class);

        this.clusterService = clusterService;
        this.client = client;
        this.indicesService = indicesService;
    }

    private BoolQueryBuilder buildWordQuery(String type, String field, String word, List<String> phoneticEncodings) {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery().minimumNumberShouldMatch(2)
                .filter(QueryBuilders.typeQuery(type))
//                .filter(QueryBuilders.rangeQuery("countAsFullWord").gt(0))
                .should(QueryBuilders.termQuery(field, word).boost(100.0f));

        phoneticEncodings.stream().forEach(w -> {
            if (word.equals(w)) {
                w = "e#" + w;
            }

            TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery(field, w);
            if (w.startsWith("e#")) {
                termQueryBuilder.boost(10.0f);
            }

            boolQueryBuilder.should(termQueryBuilder);
        });

//        logger.info("BoolQueryBuilder for type: {}, field: {}, word: {}, phoneticEncodings: {} = {}", type, field, word, phoneticEncodings, boolQueryBuilder.toString());

        return boolQueryBuilder;
    }

    @SuppressWarnings("unchecked")
    private MatchLevel matchLevel(Map<String, Object> source, String encodingField, String inputWord) {
        boolean foundEdgeGramMatch = false;
        List<Object> encodings = (List<Object>) source.get(encodingField);
        for (Object encoding : encodings) {
            if (inputWord.equals(encoding)) {
                // we have edgeGram match
                // we suggest this word
                foundEdgeGramMatch = true;
                break;
            }
        }

        if (foundEdgeGramMatch) {
            return MatchLevel.EdgeGram;
        }

        return null;
    }

    @SuppressWarnings("unchecked")
    private String[] bestDoublet(List<String> inputWords, MultiSearchResponse.Item[] responses, int wordPosition) {
        // doublet would be position = inputWords.size() + wordPosition
        int doubletPosition = inputWords.size() + wordPosition;
        if (doubletPosition >= responses.length || responses[doubletPosition].isFailure()) {
            return null;
        }

        String word1 = inputWords.get(wordPosition);
        String word2 = inputWords.get(wordPosition + 1);

        String word1WithEdgeGramPrefix = "e#" + word1;
        String word2WithEdgeGramPrefix = "e#" + word2;

        SearchResponse searchResponse = responses[doubletPosition].getResponse();

        String edgeGramSuggestion = null;
        long edgeGramSuggestionCount = 0;
        MatchLevel edgeGramSuggestionWord1MatchLevel = MatchLevel.EdgeGram;
        MatchLevel edgeGramSuggestionWord2MatchLevel = MatchLevel.EdgeGram;

        String phoneticSuggestion = null;
        int phoneticSuggestionEditDistance = Integer.MAX_VALUE;
        int phoneticSuggestionFuzzySimilarity = Integer.MIN_VALUE;
        long phoneticSuggestionCount = 0;

        // if exact match, we are good here
        // if edgeGram match, we prefer edgeGram for word2
        // if phonetic match, we prefer one with at least 50% of count as minimum of word1 and word2
        for (SearchHit searchHit : searchResponse.getHits().getHits()) {
            // check for exact value
            Map<String, Object> source = searchHit.getSource();

            String suggestedWord1 = (String) source.get("word1");
            String suggestedWord2 = (String) source.get("word2");

            MatchLevel word1MatchLevel = null;
            MatchLevel word2MatchLevel = null;

            if (word1.equals(suggestedWord1) && word2.equals(suggestedWord2)) {
                // we have exact match here
                return null;
            } else if (word1.equals(suggestedWord1)) {
                word1MatchLevel = MatchLevel.Exact;
            } else if (word2.equals(suggestedWord2)) {
                word2MatchLevel = MatchLevel.Exact;
            }

            int totalCount = (int) source.get("totalCount");

            if (word1MatchLevel == null) {
                word1MatchLevel = matchLevel(source, "word1Encodings", word1WithEdgeGramPrefix);
            }

            if (word2MatchLevel == null) {
                word2MatchLevel = matchLevel(source, "word2Encodings", word2WithEdgeGramPrefix);
            }

            if (word1MatchLevel != null && word2MatchLevel != null) {
                // prefer
                // (1) the one with word1 exact and word2 edgeGram
                // (2) one of the exact and another edgeGram
                // (3) both edgeGram
                if (word1MatchLevel == MatchLevel.Exact && (edgeGramSuggestionWord1MatchLevel != MatchLevel.Exact || totalCount > edgeGramSuggestionCount)
                        || word2MatchLevel.level < edgeGramSuggestionWord2MatchLevel.level
                        || word2MatchLevel.level == edgeGramSuggestionWord2MatchLevel.level && totalCount > edgeGramSuggestionCount) {
                    edgeGramSuggestion = suggestedWord1 + " " + suggestedWord2;
                    edgeGramSuggestionCount = totalCount;
                    edgeGramSuggestionWord1MatchLevel = word1MatchLevel;
                    edgeGramSuggestionWord2MatchLevel = word2MatchLevel;
                }
            }

            // we select the word with proper edit distance
            int distance = StringUtils.getLevenshteinDistance(suggestedWord1, word1) + StringUtils.getLevenshteinDistance(suggestedWord2, word2);
            int similarity = StringUtils.getFuzzyDistance(suggestedWord1, word1, Locale.ENGLISH) + StringUtils.getFuzzyDistance(suggestedWord2, word2, Locale.ENGLISH);
            if (distance == phoneticSuggestionEditDistance && phoneticSuggestionCount < totalCount && phoneticSuggestionFuzzySimilarity < similarity || distance < phoneticSuggestionEditDistance) {
                phoneticSuggestion = suggestedWord1 + " " + suggestedWord2;
                phoneticSuggestionCount = totalCount;
                phoneticSuggestionEditDistance = distance;
                phoneticSuggestionFuzzySimilarity = similarity;
            }
        }

        // return suggestions here
        if (edgeGramSuggestion != null || phoneticSuggestion != null) {
            if (StringUtils.equals(edgeGramSuggestion, phoneticSuggestion)) {
                return new String[]{edgeGramSuggestion};
            } else {
                if (edgeGramSuggestion != null && phoneticSuggestion != null) {
                    return new String[]{edgeGramSuggestion, phoneticSuggestion};
                }
                if (edgeGramSuggestion != null) {
                    return new String[]{edgeGramSuggestion};
                }

                if (phoneticSuggestion != null) {
                    return new String[]{phoneticSuggestion};
                }
            }
        }

        return null;
    }

    private void bestTriplet() {

    }

    @SuppressWarnings("unchecked")
    private DidYouMeanResponse singleIndexResults(List<String> words, Map<String, List<String>> wordToEncodings, long startTime, String... didYouMeanIndex) {
        List<SearchRequestBuilder> unigramRequestBuilders = new LinkedList<>();
        List<SearchRequestBuilder> bigramRequestBuilders = new LinkedList<>();

        String previousWord = null;
        List<String> previousEncodings = null;

        for (String word : words) {
            List<String> encodings = wordToEncodings.get(word);

            unigramRequestBuilders.add(client.prepareSearch(didYouMeanIndex)
                    .setQuery(buildWordQuery(UNIGRAM_DID_YOU_MEAN_INDEX_TYPE, "encodings", word, encodings)));

            if (previousWord != null) {
                bigramRequestBuilders.add(client.prepareSearch(didYouMeanIndex)
                        .setQuery(QueryBuilders.boolQuery()
                                .must(buildWordQuery(BIGRAM_DID_YOU_MEAN_INDEX_TYPE, "word1Encodings", previousWord, previousEncodings))
                                .must(buildWordQuery(BIGRAM_DID_YOU_MEAN_INDEX_TYPE, "word2Encodings", word, encodings))));
            }

            previousWord = word;
            previousEncodings = encodings;
        }

        int wordCount = words.size();

        ActionRequestBuilder searchRequestBuilder;
        if (wordCount == 1) {
            // we fire only one request
            searchRequestBuilder = unigramRequestBuilders.get(0);
        } else {
            MultiSearchRequestBuilder multiSearchRequestBuilder = client.prepareMultiSearch();

            unigramRequestBuilders.forEach(multiSearchRequestBuilder::add);
            bigramRequestBuilders.forEach(multiSearchRequestBuilder::add);

            searchRequestBuilder = multiSearchRequestBuilder;
        }

        Object actionResponse = searchRequestBuilder.execute().actionGet();
        if (wordCount > 1) {
            MultiSearchResponse multiSearchResponse = (MultiSearchResponse) actionResponse;
            // logger.info("MultiSearchResponse: {}", multiSearchResponse);

            if (wordCount == 2) {
                // get word1 + word2
                // if exact match, we are good here
                // if edgeGram match, we prefer edgeGram for word2
                // if phonetic match, we prefer one with at least 50% of count as minimum of word1 and word2
                String[] suggestions = bestDoublet(words, multiSearchResponse.getResponses(), 0);
                if (suggestions != null) {
                    int totalResults = suggestions.length;
                    DidYouMeanResult[] didYouMeanResults = new DidYouMeanResult[totalResults];
                    for (int i = 0; i < suggestions.length; i++) {
                        didYouMeanResults[i] = new DidYouMeanResult(suggestions[i]);
                    }

                    SearchResponse searchResponse = multiSearchResponse.getResponses()[0].getResponse();

                    return new DidYouMeanResponse(
                            didYouMeanResults,
                            searchResponse.getTotalShards(),
                            searchResponse.getSuccessfulShards(),
                            Math.max(1, System.currentTimeMillis() - startTime),
                            searchResponse.getShardFailures());
                }
            } else if (wordCount == 3) {
                // get word1 + word2, word2 + word3
                // for us to suggest we require word1 + word2 + word3 occur
                // if exact match of word1 + word2 and word2 + word3, we are good here
                // if edgeGram match, we are still better => better is edgeGram match in only one
                // if phonetic match, we prefer one with both of doublets having at least 50% of count as minimum of word1 and word2
            } else {
                // how to make algorithm recursive by using wordCount == 3 one
                // for now we may choose not to return any suggestion for this
                // we suggest only in one triplet, if at all
            }

        } else {
            SearchResponse searchResponse = (SearchResponse) actionResponse;

            String inputWord = words.get(0);

            String wordWithEdgeGramPrefix = "e#" + inputWord;

            SearchHits searchHits = searchResponse.getHits();

            String edgeGramSuggestion = null;
            long edgeGramSuggestionCount = 0;

            String phoneticSuggestion = null;
            int phoneticSuggestionEditDistance = Integer.MAX_VALUE;
            int phoneticSuggestionFuzzySimilarity = Integer.MIN_VALUE;
            long phoneticSuggestionCount = 0;

            for (SearchHit searchHit : searchHits.getHits()) {
                // check for exact value
                Map<String, Object> source = searchHit.getSource();

                String suggestedWord = (String) source.get("word");

                if (inputWord.equals(suggestedWord)) {
                    // we have exact match here
                    break;
                }

                int totalCount = (int) source.get("totalCount");

                boolean foundEdgeGramMatch = false;
                List<Object> encodings = (List<Object>) source.get("encodings");
                for (Object encoding : encodings) {
                    if (wordWithEdgeGramPrefix.equals(encoding)) {
                        // we have edgeGram match
                        // we suggest this word
                        foundEdgeGramMatch = true;
                        break;
                    }
                }

                if (foundEdgeGramMatch && totalCount > edgeGramSuggestionCount) {
                    edgeGramSuggestion = suggestedWord;
                    edgeGramSuggestionCount = totalCount;
                }

                // we select the word with proper edit distance
                int distance = StringUtils.getLevenshteinDistance(suggestedWord, inputWord);
                int similarity = StringUtils.getFuzzyDistance(suggestedWord, inputWord, Locale.ENGLISH);
                if (distance == phoneticSuggestionEditDistance && phoneticSuggestionCount < totalCount && phoneticSuggestionFuzzySimilarity < similarity || distance < phoneticSuggestionEditDistance) {
                    phoneticSuggestion = suggestedWord;
                    phoneticSuggestionCount = totalCount;
                    phoneticSuggestionEditDistance = distance;
                    phoneticSuggestionFuzzySimilarity = similarity;
                }
            }

            // add the suggestions to result
            if (edgeGramSuggestion != null || phoneticSuggestion != null) {
                List<DidYouMeanResult> didYouMeanResultList = new ArrayList<>();
                if (StringUtils.equals(edgeGramSuggestion, phoneticSuggestion)) {
                    didYouMeanResultList.add(new DidYouMeanResult(phoneticSuggestion));
                } else {
                    if (edgeGramSuggestion != null) {
                        didYouMeanResultList.add(new DidYouMeanResult(edgeGramSuggestion));
                    }

                    if (phoneticSuggestion != null) {
                        didYouMeanResultList.add(new DidYouMeanResult(phoneticSuggestion));
                    }
                }

                int totalResults = didYouMeanResultList.size();

                return new DidYouMeanResponse(
                        didYouMeanResultList.toArray(new DidYouMeanResult[totalResults]),
                        searchResponse.getTotalShards(),
                        searchResponse.getSuccessfulShards(),
                        Math.max(1, System.currentTimeMillis() - startTime),
                        searchResponse.getShardFailures());
            }
        }

        return new DidYouMeanResponse(Math.max(1, System.currentTimeMillis() - startTime));
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void doExecute(DidYouMeanRequest didYouMeanRequest, ActionListener<DidYouMeanResponse> listener) {
        long startTime = System.currentTimeMillis();

        threadPool.executor(ThreadPool.Names.SEARCH).execute(new ActionRunnable<DidYouMeanResponse>(listener) {
            @Override
            public void doRun() throws IOException {
                TokenEncodingUtility tokenEncodingUtility = new TokenEncodingUtility();

                ClusterState clusterState = clusterService.state();

                String[] inputIndices = indexNameExpressionResolver.concreteIndices(clusterState, didYouMeanRequest);

                // resolve these too
                String[] didYouMeanIndices = indexNameExpressionResolver.concreteIndices(clusterState,
                        didYouMeanRequest.indicesOptions(),
                        Arrays.stream(indexNameExpressionResolver.concreteIndices(clusterState, didYouMeanRequest)).map(value -> value + ":did_you_mean_store").toArray(String[]::new));

                // Map<String, Set<String>> routingMap = indexNameExpressionResolver.resolveSearchRouting(clusterState, null, didYouMeanIndices);
                // int shardCount = clusterService.operationRouting().searchShardsCount(clusterState, didYouMeanIndices, routingMap);

                // String didYouMeanIndex = didYouMeanIndices[0];

                // TODO: hand construct the analyzer... rather than expect in indices
                IndexService indexService = indicesService.indexService(inputIndices[0]);
                Analyzer didYouMeanBuilderAnalyzer = indexService.analysisService().analyzer("humane_did_you_mean_builder_analyzer");

                if (didYouMeanBuilderAnalyzer == null) {
                    logger.error("No analyzer found for indices: {}, indexService: {}, analysisService: {}", inputIndices[0], indexService, indexService.analysisService());
                    return;
                }

                TokenStream tokenStream = didYouMeanBuilderAnalyzer.tokenStream("dummyField", didYouMeanRequest.query());
                tokenStream.reset();
                CharTermAttribute termAttribute = tokenStream.getAttribute(CharTermAttribute.class);

                List<String> words = new LinkedList<>();
                Map<String, List<String>> wordToEncodings = new HashMap<>();
                while (tokenStream.incrementToken()) {
                    String word = termAttribute.toString();

                    words.add(word);

                    List<String> encodings = tokenEncodingUtility.buildEncodings(word);
                    wordToEncodings.put(word, encodings);
                }

                tokenStream.close();

//                int indicesCount = didYouMeanIndices.length;

//                if (indicesCount == 1) {
                    listener.onResponse(singleIndexResults(words, wordToEncodings, startTime, didYouMeanIndices));
//                } else {
//                    int totalShards = 0;
//                    int successfulShards = 0;
//
//                    List<ShardSearchFailure> mergedShardSearchFailures = new ArrayList<>();
//                    List<DidYouMeanResult> mergedResultList = new ArrayList<>();
//
//                    for (String index : didYouMeanIndices) {
//                        DidYouMeanResponse response = singleIndexResults(words, wordToEncodings, index, startTime);
//
//                        totalShards += response.getTotalShards();
//                        successfulShards += response.getSuccessfulShards();
//
//                        Collections.addAll(mergedShardSearchFailures, response.getShardFailures());
//
//                        if (response.getResults().length == 0) {
//                            listener.onResponse(new DidYouMeanResponse(Math.max(1, System.currentTimeMillis() - startTime)));
//                            break;
//                        }
//
//                        for (DidYouMeanResult result : response.getResults()) {
//                            boolean found = false;
//                            for (DidYouMeanResult existingResult : mergedResultList) {
//                                if (StringUtils.equals(result.getResult(), existingResult.getResult())) {
//                                    found = true;
//                                    break;
//                                }
//                            }
//
//                            if (!found) {
//                                mergedResultList.add(result);
//                            }
//                        }
//                    }
//
//                    listener.onResponse(
//                            new DidYouMeanResponse(
//                                    mergedResultList.toArray(new DidYouMeanResult[mergedResultList.size()]),
//                                    totalShards,
//                                    successfulShards,
//                                    Math.max(1, System.currentTimeMillis() - startTime),
//                                    mergedShardSearchFailures.toArray(new ShardSearchFailure[mergedShardSearchFailures.size()])
//                            ));
//                }
            }

            @Override
            public void onFailure(Throwable t) {
                logger.debug("failed to execute didYouMean", t);
                super.onFailure(t);
            }
        });
    }

    static enum MatchLevel {
        Exact(0),
        EdgeGram(1),
        Phonetic(2),
        EdgeGramPhonetic(3);

        int level;

        MatchLevel(int level) {
            this.level = level;
        }
    }
}

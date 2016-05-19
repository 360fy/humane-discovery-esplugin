package io.threesixtyfy.humaneDiscovery.didYouMean.action;

import io.threesixtyfy.humaneDiscovery.didYouMean.commons.Conjunct;
import io.threesixtyfy.humaneDiscovery.didYouMean.commons.Disjunct;
import io.threesixtyfy.humaneDiscovery.didYouMean.commons.DisjunctsBuilder;
import io.threesixtyfy.humaneDiscovery.didYouMean.commons.MatchLevel;
import io.threesixtyfy.humaneDiscovery.didYouMean.commons.Suggestion;
import io.threesixtyfy.humaneDiscovery.didYouMean.commons.SuggestionSet;
import io.threesixtyfy.humaneDiscovery.didYouMean.commons.SuggestionsBuilder;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TransportDidYouMeanAction extends HandledTransportAction<DidYouMeanRequest, DidYouMeanResponse> {

    private final ClusterService clusterService;

    private final IndicesService indicesService;

    private final Client client;

    private final SuggestionsBuilder suggestionsBuilder = SuggestionsBuilder.INSTANCE();

    private final DisjunctsBuilder disjunctsBuilder = new DisjunctsBuilder();

    @Inject
    public TransportDidYouMeanAction(Settings settings,
                                     String actionName,
                                     ThreadPool threadPool,
                                     TransportService transportService,
                                     ClusterService clusterService,
                                     IndicesService indicesService,
                                     ActionFilters actionFilters,
                                     IndexNameExpressionResolver indexNameExpressionResolver,
                                     Client client) {
        super(settings, actionName, threadPool, transportService, actionFilters, indexNameExpressionResolver, DidYouMeanRequest.class);

        this.clusterService = clusterService;
        this.client = client;
        this.indicesService = indicesService;
    }

//    @SuppressWarnings("unchecked")
//    private MatchLevel matchLevel(Map<String, Object> source, String encodingField, String inputWord) {
//        boolean foundEdgeGramMatch = false;
//        List<Object> encodings = (List<Object>) source.get(encodingField);
//        for (Object encoding : encodings) {
//            if (inputWord.equals(encoding)) {
//                // we have edgeGram match
//                // we suggest this word
//                foundEdgeGramMatch = true;
//                break;
//            }
//        }
//
//        if (foundEdgeGramMatch) {
//            return MatchLevel.EdgeGram;
//        }
//
//        return null;
//    }
//
//    // TODO: fix this by taking in calculation from suggestions
//    @SuppressWarnings("unchecked")
//    private String[] bestDoublet(List<String> inputWords, MultiSearchResponse.Item[] responses, int wordPosition) {
//        // doublet would be position = inputWords.size() + wordPosition
//        int doubletPosition = inputWords.size() + wordPosition;
//        if (doubletPosition >= responses.length || responses[doubletPosition].isFailure()) {
//            return null;
//        }
//
//        String word1 = inputWords.get(wordPosition);
//        String word2 = inputWords.get(wordPosition + 1);
//
//        String word1WithEdgeGramPrefix = Constants.EdgeGramPrefix + word1;
//        String word2WithEdgeGramPrefix = Constants.EdgeGramPrefix + word2;
//
//        SearchResponse searchResponse = responses[doubletPosition].getResponse();
//
//        String edgeGramSuggestion = null;
//        long edgeGramSuggestionCount = 0;
//        MatchLevel edgeGramSuggestionWord1MatchLevel = MatchLevel.EdgeGram;
//        MatchLevel edgeGramSuggestionWord2MatchLevel = MatchLevel.EdgeGram;
//
//        String phoneticSuggestion = null;
//        int phoneticSuggestionEditDistance = Integer.MAX_VALUE;
//        int phoneticSuggestionFuzzySimilarity = Integer.MIN_VALUE;
//        long phoneticSuggestionCount = 0;
//
//        // if exact match, we are good here
//        // if edgeGram match, we prefer edgeGram for word2
//        // if phonetic match, we prefer one with at least 50% of count as minimum of word1 and word2
//        for (SearchHit searchHit : searchResponse.getHits().getHits()) {
//            // check for exact suggestion
//            Map<String, Object> source = searchHit.getSource();
//
//            String suggestedWord1 = (String) source.get("word1");
//            String suggestedWord2 = (String) source.get("word2");
//
//            MatchLevel word1MatchLevel = null;
//            MatchLevel word2MatchLevel = null;
//
//            if (word1.equals(suggestedWord1) && word2.equals(suggestedWord2)) {
//                // we have exact match here
//                return null;
//            } else if (word1.equals(suggestedWord1)) {
//                word1MatchLevel = MatchLevel.Exact;
//            } else if (word2.equals(suggestedWord2)) {
//                word2MatchLevel = MatchLevel.Exact;
//            }
//
//            int totalCount = (int) source.get("totalCount");
//
//            if (word1MatchLevel == null) {
//                word1MatchLevel = matchLevel(source, "word1Encodings", word1WithEdgeGramPrefix);
//            }
//
//            if (word2MatchLevel == null) {
//                word2MatchLevel = matchLevel(source, "word2Encodings", word2WithEdgeGramPrefix);
//            }
//
//            if (word1MatchLevel != null && word2MatchLevel != null) {
//                // prefer
//                // (1) the one with word1 exact and word2 edgeGram
//                // (2) one of the exact and another edgeGram
//                // (3) both edgeGram
//                if (word1MatchLevel == MatchLevel.Exact && (edgeGramSuggestionWord1MatchLevel != MatchLevel.Exact || totalCount > edgeGramSuggestionCount)
//                        || word2MatchLevel.getLevel() < edgeGramSuggestionWord2MatchLevel.getLevel()
//                        || word2MatchLevel.getLevel() == edgeGramSuggestionWord2MatchLevel.getLevel() && totalCount > edgeGramSuggestionCount) {
//                    edgeGramSuggestion = suggestedWord1 + " " + suggestedWord2;
//                    edgeGramSuggestionCount = totalCount;
//                    edgeGramSuggestionWord1MatchLevel = word1MatchLevel;
//                    edgeGramSuggestionWord2MatchLevel = word2MatchLevel;
//                }
//            }
//
//            // we select the word with proper edit distance
//            int distance = StringUtils.getLevenshteinDistance(suggestedWord1, word1) + StringUtils.getLevenshteinDistance(suggestedWord2, word2);
//            int similarity = StringUtils.getFuzzyDistance(suggestedWord1, word1, Locale.ENGLISH) + StringUtils.getFuzzyDistance(suggestedWord2, word2, Locale.ENGLISH);
//            if (distance == phoneticSuggestionEditDistance && phoneticSuggestionCount < totalCount && phoneticSuggestionFuzzySimilarity < similarity || distance < phoneticSuggestionEditDistance) {
//                phoneticSuggestion = suggestedWord1 + " " + suggestedWord2;
//                phoneticSuggestionCount = totalCount;
//                phoneticSuggestionEditDistance = distance;
//                phoneticSuggestionFuzzySimilarity = similarity;
//            }
//        }
//
//        // return suggestions here
//        if (edgeGramSuggestion != null || phoneticSuggestion != null) {
//            if (StringUtils.equals(edgeGramSuggestion, phoneticSuggestion)) {
//                return new String[]{edgeGramSuggestion};
//            } else {
//                if (edgeGramSuggestion != null && phoneticSuggestion != null) {
//                    return new String[]{edgeGramSuggestion, phoneticSuggestion};
//                }
//                if (edgeGramSuggestion != null) {
//                    return new String[]{edgeGramSuggestion};
//                }
//
//                if (phoneticSuggestion != null) {
//                    return new String[]{phoneticSuggestion};
//                }
//            }
//        }
//
//        return null;
//    }

    @SuppressWarnings("unchecked")
    private DidYouMeanResponse buildResponse(List<String> tokens, long startTime, String... didYouMeanIndex) {
        int wordCount = tokens.size();

        Map<String, Conjunct> conjunctMap = new HashMap<>();
        Set<Disjunct> disjuncts = disjunctsBuilder.build(tokens, conjunctMap);

        Map<String, SuggestionSet> suggestionsMap = suggestionsBuilder.fetchSuggestions(this.client, conjunctMap.values(), didYouMeanIndex);
        if (suggestionsMap == null) {
            return new DidYouMeanResponse(Math.max(1, System.currentTimeMillis() - startTime));
        }

        if (wordCount > 1) {
//            MultiSearchResponse multiSearchResponse = (MultiSearchResponse) actionResponse;
//            // logger.info("MultiSearchResponse: {}", multiSearchResponse);
//
//            // TODO: from each response build Suggestions and collate them to build multi word suggestions
//
//            if (wordCount == 2) {
//                // get word1 + word2
//                // if exact match, we are good here
//                // if edgeGram match, we prefer edgeGram for word2
//                // if phonetic match, we prefer one with at least 50% of count as minimum of word1 and word2
////                String[] suggestions = bestDoublet(words, multiSearchResponse.getResponses(), 0);
////                if (suggestions != null) {
////                    int totalResults = suggestions.length;
////                    DidYouMeanResult[] didYouMeanResults = new DidYouMeanResult[totalResults];
////                    for (int i = 0; i < suggestions.length; i++) {
////                        didYouMeanResults[i] = new DidYouMeanResult(suggestions[i]);
////                    }
////
////                    SearchResponse searchResponse = multiSearchResponse.getResponses()[0].getResponse();
////
////                    return new DidYouMeanResponse(
////                            didYouMeanResults,
////                            searchResponse.getTotalShards(),
////                            searchResponse.getSuccessfulShards(),
////                            Math.max(1, System.currentTimeMillis() - startTime),
////                            searchResponse.getShardFailures());
////                }
//            } else if (wordCount == 3) {
//                // get word1 + word2, word2 + word3
//                // for us to suggest we require word1 + word2 + word3 occur
//                // if exact match of word1 + word2 and word2 + word3, we are good here
//                // if edgeGram match, we are still better => better is edgeGram match in only one
//                // if phonetic match, we prefer one with both of doublets having at least 50% of count as minimum of word1 and word2
//            } else {
//                // how to make algorithm recursive by using wordCount == 3 one
//                // for now we may choose not to return any suggestion for this
//                // we suggest only in one triplet, if at all
//            }

        } else {
//            SearchResponse searchResponse = (SearchResponse) actionResponse;
//
//            String inputWord = tokens.get(0);
//            Set<Suggestion> suggestions = suggestionsBuilder.unigramSuggestions(inputWord, searchResponse);
//
//            logger.info("======> Input Word: {}, Suggestions: {}", inputWord, suggestions);
//            DidYouMeanResult[] results = suggestions.stream()
//                    .map(Suggestion::getSuggestion)
//                    .filter(suggestion -> !suggestion.equals(inputWord))
//                    .map(DidYouMeanResult::new)
//                    .toArray(DidYouMeanResult[]::new);
//
//            return new DidYouMeanResponse(
//                    results,
////                    searchResponse.getTotalShards(),
////                    searchResponse.getSuccessfulShards(),
//                    Math.max(1, System.currentTimeMillis() - startTime)
////                    searchResponse.getShardFailures()
//            );

            String inputWord = tokens.get(0);
            SuggestionSet suggestionSet = suggestionsMap.get(inputWord);

//            logger.info("======> Input Word: {}, Suggestions: {}", inputWord, suggestionSet);

            if (suggestionSet != null && suggestionSet.getSuggestions() != null) {
                boolean hasExactMatch = false;
                boolean hasEdgeGramMatch = false;
                for (Suggestion suggestion : suggestionSet.getSuggestions()) {
                    if (suggestion.getMatchLevel() == MatchLevel.EdgeGram) {
                        hasEdgeGramMatch = true;
                    } else if (suggestion.getMatchLevel() == MatchLevel.Exact) {
                        hasExactMatch = true;
                    }
                }

                if (!hasExactMatch || hasEdgeGramMatch) {
//                    boolean finalHasEdgeGramMatch = hasEdgeGramMatch;
                    DidYouMeanResult[] results = suggestionSet.getSuggestions().stream()
//                            .filter(suggestion -> !finalHasEdgeGramMatch || (suggestion.getMatchLevel() == MatchLevel.EdgeGram || suggestion.getMatchLevel() == MatchLevel.Exact))
                            .map(Suggestion::getSuggestion)
                            .map(DidYouMeanResult::new)
                            .toArray(DidYouMeanResult[]::new);

                    return new DidYouMeanResponse(results, Math.max(1, System.currentTimeMillis() - startTime));
                }
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
            protected void doRun() throws Exception {
                try {
                    ClusterState clusterState = clusterService.state();

                    String[] inputIndices = indexNameExpressionResolver.concreteIndices(clusterState, didYouMeanRequest);

                    // resolve these too
                    String[] didYouMeanIndices = indexNameExpressionResolver.concreteIndices(clusterState,
                            didYouMeanRequest.indicesOptions(),
                            Arrays.stream(indexNameExpressionResolver.concreteIndices(clusterState, didYouMeanRequest)).map(value -> value + ":did_you_mean_store").toArray(String[]::new));

                    IndexService indexService = indicesService.indexService(inputIndices[0]);

                    if (indexService == null) {
                        logger.error("IndexService is null for: {}", inputIndices[0]);
                        throw new IOException("IndexService is not found for: " + inputIndices[0]);
                    }

                    List<String> words = suggestionsBuilder.tokens(indexService.analysisService(), didYouMeanRequest.query());

                    listener.onResponse(buildResponse(words, startTime, didYouMeanIndices));
                } catch (IOException e) {
                    logger.error("failed to execute didYouMean", e);
                    listener.onFailure(e);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                logger.error("failed to execute didYouMean", t);
                super.onFailure(t);
            }
        });
    }
}

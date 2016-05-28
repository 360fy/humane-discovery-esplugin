package io.threesixtyfy.humaneDiscovery.didYouMean.action;

import io.threesixtyfy.humaneDiscovery.didYouMean.commons.Conjunct;
import io.threesixtyfy.humaneDiscovery.didYouMean.commons.Disjunct;
import io.threesixtyfy.humaneDiscovery.didYouMean.commons.DisjunctsBuilder;
import io.threesixtyfy.humaneDiscovery.didYouMean.commons.MatchLevel;
import io.threesixtyfy.humaneDiscovery.didYouMean.commons.Suggestion;
import io.threesixtyfy.humaneDiscovery.didYouMean.commons.SuggestionSet;
import io.threesixtyfy.humaneDiscovery.didYouMean.commons.SuggestionUtils;
import io.threesixtyfy.humaneDiscovery.didYouMean.commons.SuggestionsBuilder;
import org.apache.commons.lang3.StringUtils;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

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

    @SuppressWarnings("unchecked")
    private DidYouMeanResponse buildResponse(List<String> tokens, long startTime, String... didYouMeanIndex) {
        Map<String, Conjunct> conjunctMap = new HashMap<>();
        Disjunct[] disjuncts = disjunctsBuilder.build(tokens, conjunctMap);

        if (disjuncts == null) {
            return emptyResponse(startTime);
        }

        Map<String, SuggestionSet> suggestionsMap = suggestionsBuilder.fetchSuggestions(this.client, conjunctMap.values(), didYouMeanIndex);
        if (suggestionsMap == null) {
            return emptyResponse(startTime);
        }

        // order disjunct by their weights based on suggestion
        // if there is only one disjunct... we can simply pick the best suggestion and be happy
        if (disjuncts.length == 1) {
            // this would be when there is single token... and single conjunct
            Disjunct disjunct = disjuncts[0];
            Conjunct conjunct = disjunct.getConjuncts()[0];

            SuggestionSet suggestionSet = suggestionsMap.get(conjunct.getKey());

            // TODO: support flag whether edge matches should be returned as suggestions
            // get suggestion as normal word and suggestion as joined
            if (!SuggestionUtils.noSuggestions(suggestionSet)) {
                Suggestion[] suggestions = suggestionSet.getSuggestions();

                boolean hasExactMatch = false;
                boolean unigram = true;
                for (Suggestion suggestion : suggestions) {
                    if (suggestion.getMatchLevel() == MatchLevel.Exact) {
                        hasExactMatch = true;
                        if (suggestion.getTokenType() == Suggestion.TokenType.Bi) {
                            unigram = false;
                        } else if (!unigram && suggestion.getTokenType() == Suggestion.TokenType.Uni) {
                            unigram = true;
                        }
                    }
                }

                if (hasExactMatch && !unigram) {
                    int size = 1;
                    DidYouMeanResult[] results = new DidYouMeanResult[size];

                    for (Suggestion suggestion : suggestions) {
                        if (suggestion.getMatchLevel() == MatchLevel.Exact && suggestion.getTokenType() == Suggestion.TokenType.Bi) {
                            results[0] = new DidYouMeanResult(suggestion.getDisplay());
                        }
                    }

                    return new DidYouMeanResponse(results, Math.max(1, System.currentTimeMillis() - startTime));
                }

                // but add bigram, even if exact
                if (!hasExactMatch) {
                    int size = Math.min(suggestions.length, 5);
                    DidYouMeanResult[] results = new DidYouMeanResult[size];

                    for (int i = 0; i < size; i++) {
                        results[i] = new DidYouMeanResult(suggestions[i].getDisplay());
                    }

                    return new DidYouMeanResponse(results, Math.max(1, System.currentTimeMillis() - startTime));
                }
            }
        } else {
            SortedSet<DidYouMeanSuggestion> allSuggestions = new TreeSet<>();

            for (Disjunct disjunct : disjuncts) {
                // check if we can create a suggestion out of the disjunct
                // premise one and only one conjunct shall have non exact suggestion
                Conjunct[] conjuncts = disjunct.getConjuncts();
                int conjunctCount = conjuncts.length;

                int nonExactSuggestion = 0;
                int nonExactConjunctIndex = 0;
                SuggestionSet nonExactSuggestionSet = null;
                for (int i = 0; i < conjunctCount; i++) {
                    Conjunct conjunct = conjuncts[i];
                    SuggestionSet suggestionSet = suggestionsMap.get(conjunct.getKey());
                    if (SuggestionUtils.noSuggestions(suggestionSet)) {
                        if (suggestionSet != null && !suggestionSet.isNumber()) {
                            nonExactSuggestion++;
                        }

                        continue;
                    }

                    Suggestion firstSuggestion = suggestionSet.getSuggestions()[0];
                    if (firstSuggestion.getMatchLevel() != MatchLevel.Exact) {
                        nonExactSuggestionSet = suggestionSet;
                        nonExactConjunctIndex = i;
                        nonExactSuggestion++;
                    } else if (conjunct.getLength() == 1) {
                        // find out another conjunct of size 2 where it is one of the token and does not have exact match
                        for (Conjunct c : conjunctMap.values()) {
                            if (c.getLength() > 1 && c.getTokens().contains(conjunct.getKey())) {
                                SuggestionSet ss = suggestionsMap.get(c.getKey());
                                if (!SuggestionUtils.noSuggestions(ss) && ss.getSuggestions()[0].getMatchLevel() != MatchLevel.Exact) {
                                    nonExactSuggestion++;
                                    break;
                                }
                            }
                        }
                    }
                }

                if (nonExactSuggestion > 1 || nonExactSuggestionSet == null) {
                    continue;
                }

                StringBuilder prefix = new StringBuilder();
                for (int i = 0; i < nonExactConjunctIndex; i++) {
                    if (i > 0) {
                        prefix.append(" ");
                    }

                    prefix.append(StringUtils.join(conjuncts[i].getTokens(), " "));
                }

                // build suffix string
                StringBuilder suffix = new StringBuilder();
                for (int i = nonExactConjunctIndex + 1; i < conjunctCount; i++) {
                    if (i > nonExactConjunctIndex + 1) {
                        suffix.append(" ");
                    }

                    suffix.append(StringUtils.join(conjuncts[i].getTokens(), " "));
                }

                for (Suggestion suggestion : nonExactSuggestionSet.getSuggestions()) {
                    String suggestText = Arrays.asList(prefix.toString(), suggestion.getDisplay(), suffix.toString()).stream()
                            .filter(val -> val != null && !StringUtils.isEmpty(val))
                            .collect(Collectors.joining(" "));

                    // score as per the picked suggestion
                    float suggestionScore = getSuggestionScore(conjuncts[nonExactConjunctIndex].getLength(), suggestion);

                    allSuggestions.add(new DidYouMeanSuggestion(suggestText, suggestionScore));
                }
            }

            if (allSuggestions.size() > 0) {
                List<String> suggestions = new ArrayList<>();
                for (DidYouMeanSuggestion didYouMeanSuggestion : allSuggestions) {
                    if (StringUtils.equalsIgnoreCase(didYouMeanSuggestion.suggestion, StringUtils.join(tokens, " "))) {
                        break;
                    }

                    suggestions.add(didYouMeanSuggestion.suggestion);
                }

                if (suggestions.size() > 0) {
                    int size = Math.min(suggestions.size(), 5);
                    DidYouMeanResult[] results = new DidYouMeanResult[size];

                    for (int i = 0; i < size; i++) {
                        results[i] = new DidYouMeanResult(suggestions.get(i));
                    }

                    return new DidYouMeanResponse(results, Math.max(1, System.currentTimeMillis() - startTime));
                }
            }

        }

        return emptyResponse(startTime);
    }

    private float getSuggestionScore(int conjunctTokens, Suggestion suggestion) {
        float suggestionScore = 1.0f;

        if (suggestion.getMatchLevel() == MatchLevel.Exact) {
            suggestionScore = 1.0f;
        } else if (suggestion.getMatchLevel() == MatchLevel.EdgeGram) {
            suggestionScore = 0.5f;
        } else if (suggestion.getMatchLevel() == MatchLevel.Phonetic) {
            suggestionScore = 0.1f;
        } else if (suggestion.getMatchLevel() == MatchLevel.EdgeGramPhonetic) {
            suggestionScore = 0.05f;
        }

        suggestionScore = (1 - suggestion.getEditDistance() / 10.0f) * suggestionScore;

        if (suggestion.getTokenType() == Suggestion.TokenType.Bi || conjunctTokens > 1) {
            suggestionScore = suggestionScore * 2.0f;
        }

        return suggestionScore;
    }

    private DidYouMeanResponse emptyResponse(long startTime) {
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

    static class DidYouMeanSuggestion implements Comparable<DidYouMeanSuggestion> {
        String suggestion;
        float score;

        public DidYouMeanSuggestion(String suggestion, float score) {
            this.suggestion = suggestion;
            this.score = score;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            DidYouMeanSuggestion that = (DidYouMeanSuggestion) o;

            return suggestion.equals(that.suggestion);

        }

        @Override
        public int hashCode() {
            return suggestion.hashCode();
        }

        @Override
        public int compareTo(DidYouMeanSuggestion o) {
            int ret = suggestion.compareTo(o.suggestion);

            if (ret == 0) {
                return 0;
            }

            ret = (int) ((o.score - score) * 1000);

            if (ret == 0) {
                ret = suggestion.compareTo(o.suggestion);
            }

            return ret;
        }

        @Override
        public String toString() {
            return "{" +
                    "suggestion='" + suggestion + '\'' +
                    ", score=" + score +
                    '}';
        }
    }
}

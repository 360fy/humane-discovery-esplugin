package io.threesixtyfy.humaneDiscovery.api.intent;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.threesixtyfy.humaneDiscovery.core.conjuncts.Conjunct;
import io.threesixtyfy.humaneDiscovery.core.conjuncts.DisjunctsBuilder;
import io.threesixtyfy.humaneDiscovery.core.conjuncts.TokensBuilder;
import io.threesixtyfy.humaneDiscovery.core.spellSuggestion.Suggestion;
import io.threesixtyfy.humaneDiscovery.core.spellSuggestion.SuggestionSet;
import io.threesixtyfy.humaneDiscovery.core.spellSuggestion.SuggestionsBuilder;
import io.threesixtyfy.humaneDiscovery.service.wordIndex.WordIndexConstants;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.Stack;
import java.util.TreeSet;
import java.util.stream.Collectors;


// TODO: add token also, if there is no exact match
// TODO: return entity types too in suggestion
public class TransportIntentAction extends HandledTransportAction<IntentRequest, IntentResponse> {

    private static final Logger logger = Loggers.getLogger(TransportIntentAction.class);

    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
    private static final FeatureFunction[] FeatureFunctions = new FeatureFunction[]{
            new PrioritiseConsecutiveSuggestion(),
            new PrioritiseSuggestionWithMarker(),
            new PrioritiseWithBigram(),
            new DeprioritiseInterleavingSuggestion()
    };

    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final Client client;
    private final SuggestionsBuilder suggestionsBuilder = SuggestionsBuilder.INSTANCE();
    private final DisjunctsBuilder disjunctsBuilder = DisjunctsBuilder.INSTANCE();
    private final TokensBuilder tokensBuilder = TokensBuilder.INSTANCE();

    @Inject
    public TransportIntentAction(Settings settings,
                                 ThreadPool threadPool,
                                 TransportService transportService,
                                 ClusterService clusterService,
                                 IndicesService indicesService,
                                 ActionFilters actionFilters,
                                 IndexNameExpressionResolver indexNameExpressionResolver,
                                 Client client) {
        super(settings, IntentAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, IntentRequest::new);

        this.clusterService = clusterService;
        this.client = client;
        this.indicesService = indicesService;
    }

    private long tookTime(long startTime) {
        return Math.max(1, System.currentTimeMillis() - startTime);
    }


    private IntentResponse emptyResponse(List<String> tokens, long startTime) {
        if (tokens == null) {
            return new IntentResponse(tookTime(startTime));
        }

        return new IntentResponse(tokens.toArray(new String[tokens.size()]), tookTime(startTime));
    }

    private IntentResponse response(String[] tokens, IntentResult[] intentResults, long startTime) {
        return new IntentResponse(tokens, intentResults, tookTime(startTime));
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void doExecute(IntentRequest intentRequest, ActionListener<IntentResponse> listener) {
        final long startTime = System.currentTimeMillis();

        threadPool.executor(ThreadPool.Names.SEARCH).execute(new ActionRunnable<IntentResponse>(listener) {
            @Override
            protected void doRun() throws Exception {
                try {
                    ClusterState clusterState = clusterService.state();

                    Index[] inputIndices = indexNameExpressionResolver.concreteIndices(clusterState, intentRequest);

                    IndexService indexService = indicesService.indexService(inputIndices[0]);

                    if (indexService == null) {
                        logger.error("IndexService is null for: {}", inputIndices[0]);
                        throw new IOException("IndexService is not found for: " + inputIndices[0]);
                    }

                    if (logger.isDebugEnabled()) {
                        logger.debug("Intent Request: {}", intentRequest);
                    }

                    // parse with intent entities

                    List<String> tokensList = tokensBuilder.tokens(indexService.analysisService(), intentRequest.querySource().query());

                    int numTokens = tokensList == null ? 0 : tokensList.size();

                    IntentResponse intentResponse;
                    if (numTokens > 0 && numTokens < 8) {
                        String[] tokens = tokensList.toArray(new String[tokensList.size()]);

                        Map<String, Conjunct> uniqueConjuncts = new HashMap<>();
                        /*Disjunct[] disjuncts =*/
                        disjunctsBuilder.build(tokensList, uniqueConjuncts, 3);

                        Set<LookupIntentEntity> lookupIntentEntities = intentRequest.querySource().getLookupEntities();

                        Map<String, SuggestionSet> suggestionsMap = suggestionsBuilder.fetchSuggestions(client,
                                uniqueConjuncts.values(),
                                new String[]{inputIndices[0].getName() + WordIndexConstants.WORD_INDEX_STORE_SUFFIX},
                                lookupIntentEntities);

                        List<SuggestionClassPermutation> suggestionClassPermutations = new ArrayList<>();
                        generateSuggestionClassPermutations(suggestionsMap, tokens, 0, numTokens - 1, new Stack<>(), suggestionClassPermutations);

                        // apply feature functions
                        applyFeatureFunctions(suggestionClassPermutations, tokens, suggestionsMap);

                        // normalise score
                        normaliseScore(suggestionClassPermutations);

                        // sort the candidates
                        Collections.sort(suggestionClassPermutations);

                        // typically first one can be the most important

                        if (logger.isDebugEnabled()) {
                            logger.debug("Intent Candidates: {}", json(suggestionClassPermutations));
                        }

                        // consider all permutations with enough confidence
                        List<IntentResult> intentResults = suggestionClassPermutations
                                .stream()
                                .filter(suggestionClassPermutation -> suggestionClassPermutation.normalisedScore >= 0.25)
                                .map(suggestionClassPermutation -> buildIntentResult(suggestionClassPermutation))
                                .collect(Collectors.toList());

                        intentResponse = response(tokens, intentResults.toArray(new IntentResult[intentResults.size()]), startTime);
                    } else {
                        intentResponse = emptyResponse(tokensList, startTime);
                    }

                    listener.onResponse(intentResponse);
                } catch (IOException e) {
                    logger.error("failed to execute intent", e);
                    listener.onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception t) {
                logger.error("failed to execute intent", t);
                super.onFailure(t);
            }
        });
    }

    private IntentResult.IntentToken toIntentToken(Suggestion suggestion) {
        return new IntentResult.IntentToken(suggestion.getSuggestion(),
                suggestion.getDisplay(),
                suggestion.getInputTokenType(),
                suggestion.getMatchTokenType(),
                suggestion.getMatchStats().getMatchLevel(),
                suggestion.getMatchStats().getEditDistance(),
                suggestion.getMatchStats().getScore());
    }

    private IntentResult buildIntentResult(SuggestionClassPermutation suggestionClassPermutation) {
        Map<String, List<IntentResult.IntentClassResult>> intentClasses = suggestionClassPermutation.suggestionClasses
                .stream()
                .filter(suggestionClass -> suggestionClass != null)
                .flatMap(v -> v.suggestionPermutations.stream())
                .map(v -> {
                    List<IntentResult.IntentToken> intentTokens = v.suggestions.stream()
                            .map(this::toIntentToken)
                            .collect(Collectors.toList());
                    return new IntentResult.IntentClassResult(v.entityClass, intentTokens, v.normalisedScore);
                })
                .collect(Collectors.groupingBy(IntentResult.IntentClassResult::getIntentClass));

        return new IntentResult(intentClasses, suggestionClassPermutation.normalisedScore);
    }

    private void generateSuggestionClassPermutations(Map<String, SuggestionSet> suggestionsMap, String[] tokens, int depth, int maxDepth, Stack<SuggestionClass> suggestionEntityStack, Collection<SuggestionClassPermutation> suggestionClassPermutations) {
        String token = tokens[depth];
        SuggestionSet suggestionSet = suggestionsMap.get(token);

        if (suggestionSet != null && suggestionSet.getSuggestions() != null && suggestionSet.getSuggestions().length > 0) {
            Arrays.stream(suggestionSet.getSuggestions())
                    .collect(Collectors.groupingBy(Suggestion::getEntityClass))
                    .entrySet()
                    .forEach(mapEntry -> {
                        SortedSet<SuggestionPermutation> suggestionPermutations = new TreeSet<>();
                        suggestionPermutations.add(new SuggestionPermutation(mapEntry.getKey(), mapEntry.getValue()));

                        suggestionEntityStack.push(new SuggestionClass(mapEntry.getKey(), suggestionPermutations, depth));

                        if (depth < maxDepth) {
                            generateSuggestionClassPermutations(suggestionsMap, tokens, depth + 1, maxDepth, suggestionEntityStack, suggestionClassPermutations);
                        } else {
                            // dump the stack here
                            SuggestionClassPermutation suggestionClassPermutation = new SuggestionClassPermutation(suggestionEntityStack.stream().collect(Collectors.toList()));

                            suggestionClassPermutations.add(suggestionClassPermutation);
                        }

                        suggestionEntityStack.pop();
                    });
        } else if (depth < maxDepth) {
            suggestionEntityStack.push(null);

            generateSuggestionClassPermutations(suggestionsMap, tokens, depth + 1, maxDepth, suggestionEntityStack, suggestionClassPermutations);

            suggestionEntityStack.pop();
        } else {
            SuggestionClassPermutation suggestionClassPermutation = new SuggestionClassPermutation(suggestionEntityStack.stream().collect(Collectors.toList()));

            suggestionClassPermutations.add(suggestionClassPermutation);
        }
    }

    private void applyFeatureFunctions(Collection<SuggestionClassPermutation> suggestionClassPermutations, String[] tokens, Map<String, SuggestionSet> suggestionsMap) {
        suggestionClassPermutations.stream()
                .filter(suggestionClassPermutation -> suggestionClassPermutation.suggestionClasses.size() > 0)
                .forEach(suggestionClassPermutation -> {
                    for (FeatureFunction featureFunction : FeatureFunctions) {
                        featureFunction.apply(suggestionClassPermutation, tokens, suggestionsMap);
                    }
                });
    }

    private void normaliseScore(Collection<SuggestionClassPermutation> suggestionClassPermutations) {
        double sum = 0.0f;

        double max = 0.0f;
        for (SuggestionClassPermutation suggestionClassPermutation : suggestionClassPermutations) {
            max = Math.max(max, suggestionClassPermutation.weight);
        }

        if (max == 0.0f) {
            return;
        }

        for (SuggestionClassPermutation suggestionClassPermutation : suggestionClassPermutations) {
            sum += suggestionClassPermutation.normalisedScore = Math.exp(suggestionClassPermutation.weight * 10.0f / max);
        }

        if (sum > 0) {
            for (SuggestionClassPermutation suggestionClassPermutation : suggestionClassPermutations) {
                suggestionClassPermutation.normalisedScore /= sum;
            }
        }

        for (SuggestionClassPermutation suggestionClassPermutation : suggestionClassPermutations) {
            suggestionClassPermutation.suggestionClasses
                    .stream()
                    .filter(suggestionClass -> suggestionClass != null)
                    .forEach(SuggestionClass::normaliseScore);
        }
    }

    private String json(Object object) {
        return AccessController.doPrivileged(new PrivilegedAction<String>() {
            @Override
            public String run() {
                return GSON.toJson(object);
            }
        });
    }

    interface FeatureFunction {
        void apply(SuggestionClassPermutation suggestionClassPermutation, String[] tokens, Map<String, SuggestionSet> suggestionsMap);
    }

    private static class SuggestionPermutation implements Comparable<SuggestionPermutation> {
        private final String entityClass;
        private final double weight;
        private final double additionalWeight;
        // these are positional
        private final List<Suggestion> suggestions;
        private double normalisedScore = 0.0f;

        SuggestionPermutation(String entityClass, List<Suggestion> suggestions) {
            this(entityClass, suggestions, 0.0f);
        }

        SuggestionPermutation(String entityClass, List<Suggestion> suggestions, double additionalWeight) {
            this.entityClass = entityClass;
            this.suggestions = suggestions;
            this.additionalWeight = additionalWeight;
            this.weight = this.additionalWeight + this.suggestions.stream().collect(Collectors.summingDouble(Suggestion::getWeight));
        }

        @Override
        public int compareTo(SuggestionPermutation o) {
            return Double.compare(o.weight, this.weight);
        }

    }

    private static class SuggestionClass implements Comparable<SuggestionClass> {
        private final int tokenStart;
        private final int tokenEnd;

        private final String entityClass;
        // these are by weight
        private final SortedSet<SuggestionPermutation> suggestionPermutations;
        private double weightMultiplier = 1.0f;
        private double weight;

        SuggestionClass(String entityClass, SortedSet<SuggestionPermutation> suggestionPermutations, int tokenStart) {
            this(entityClass, suggestionPermutations, tokenStart, tokenStart);
        }

        SuggestionClass(String entityClass, SortedSet<SuggestionPermutation> suggestionPermutations, int tokenStart, int tokenEnd) {
            this.tokenStart = tokenStart;
            this.tokenEnd = tokenEnd;
            this.entityClass = entityClass;
            this.suggestionPermutations = suggestionPermutations;
            this.computeWeight();
        }

        void halfWeightMultiplier() {
            this.weightMultiplier /= 2;
            this.computeWeight();
        }

        void computeWeight() {
            this.weight = (this.tokenEnd - this.tokenStart + 1) * weightMultiplier * this.suggestionPermutations.stream()
                    .filter(suggestionPermutation -> suggestionPermutation != null)
                    .collect(Collectors.summingDouble(v -> v.weight));
        }

        private void normaliseScore() {
            double max = 0.0f;
            for (SuggestionPermutation suggestionPermutation : suggestionPermutations) {
                max = Math.max(max, suggestionPermutation.weight);
            }

            if (max == 0.0f) {
                return;
            }

            double sum = 0.0f;
            for (SuggestionPermutation suggestionPermutation : suggestionPermutations) {
                sum += suggestionPermutation.normalisedScore = Math.exp(suggestionPermutation.weight * 10.0f / max);
            }

            if (sum > 0) {
                for (SuggestionPermutation suggestionPermutation : suggestionPermutations) {
                    suggestionPermutation.normalisedScore /= sum;
                }
            }
        }

        @Override
        public int compareTo(SuggestionClass o) {
            return Double.compare(o.weight, this.weight);
        }
    }

    private static class SuggestionClassPermutation implements Comparable<SuggestionClassPermutation> {
        // these are positional
        private final List<SuggestionClass> suggestionClasses;
        private double weight = 0.0f;
        private double normalisedScore = 0.0f;

        SuggestionClassPermutation(List<SuggestionClass> suggestionClasses) {
            this.suggestionClasses = suggestionClasses;
            this.computeWeight();
        }

        void computeWeight() {
            this.weight = suggestionClasses.stream()
                    .filter(v -> v != null)
                    .collect(Collectors.summingDouble(v -> v.weight));
        }

        // all suggestion classes from startIndex to endIndex collapses into one at startIndex
        void collapse(int startIndex, int endIndex) {
            SuggestionClass collapseInto = suggestionClasses.get(startIndex);
            if (collapseInto != null) {
                List<SuggestionClass> collapseFrom = suggestionClasses.subList(startIndex + 1, endIndex);

                suggestionClasses.set(startIndex, collapse(collapseInto, collapseFrom));

                for (int i = endIndex - 1; i > startIndex; i--) {
                    suggestionClasses.set(i, null);
                }
            }

            this.computeWeight();
        }

        SuggestionClass collapse(SuggestionClass collapseInto, List<SuggestionClass> collapseFrom) {
            SortedSet<SuggestionPermutation> currentPermutations = collapseInto.suggestionPermutations;
            SuggestionClass lastSuggestionClass = collapseInto;
            for (SuggestionClass suggestionClass : collapseFrom) {
                // new permutations would be previousPermutations * permutations in suggestion class
                SortedSet<SuggestionPermutation> newPermutations = new TreeSet<>();
                for (SuggestionPermutation permutationOne : currentPermutations) {
                    for (SuggestionPermutation permutationTwo : suggestionClass.suggestionPermutations) {
                        // create new permutations
                        List<Suggestion> suggestions = new ArrayList<>();
                        suggestions.addAll(permutationOne.suggestions);
                        suggestions.addAll(permutationTwo.suggestions);
                        newPermutations.add(new SuggestionPermutation(suggestionClass.entityClass, suggestions));
                    }
                }

                currentPermutations = newPermutations;
                lastSuggestionClass = suggestionClass;
            }

            return new SuggestionClass(collapseInto.entityClass, currentPermutations, collapseInto.tokenStart, lastSuggestionClass.tokenEnd);
        }

        @Override
        public int compareTo(SuggestionClassPermutation o) {
            return Double.compare(o.normalisedScore, this.normalisedScore);
        }
    }

    private static class PrioritiseConsecutiveSuggestion implements FeatureFunction {

        @Override
        public void apply(SuggestionClassPermutation suggestionClassPermutation, String[] tokens, Map<String, SuggestionSet> suggestionsMap) {
            // if previous suggestion and this suggestion class are same then we mark it
            List<SuggestionClass> suggestionClasses = suggestionClassPermutation.suggestionClasses;
            int size = suggestionClasses.size();
            SuggestionClass previousSuggestion = suggestionClasses.get(0);
            int count = 1;
            int startIndex = -1;

            for (int i = 1; i < size; i++) {
                SuggestionClass currentSuggestion = suggestionClasses.get(i);
                if (previousSuggestion != null && currentSuggestion != null && currentSuggestion.entityClass.equals(previousSuggestion.entityClass)) {
                    if (startIndex == -1) {
                        startIndex = i - 1;
                    }

                    count++;
                } else {
                    if (count > 1) {
                        suggestionClassPermutation.collapse(startIndex, i);
                    }

                    count = 1;
                    startIndex = -1;
                }

                previousSuggestion = currentSuggestion;
            }

            if (count > 1) {
                suggestionClassPermutation.collapse(startIndex, size);
            }

            suggestionClasses.removeIf(suggestionClass -> suggestionClass == null);

            suggestionClassPermutation.computeWeight();
        }

    }

    private static class PrioritiseSuggestionWithMarker implements FeatureFunction {

        @Override
        public void apply(SuggestionClassPermutation suggestionClassPermutation, String[] tokens, Map<String, SuggestionSet> suggestionsMap) {

        }

    }

    private static class DeprioritiseInterleavingSuggestion implements FeatureFunction {

        @Override
        public void apply(SuggestionClassPermutation suggestionClassPermutation, String[] tokens, Map<String, SuggestionSet> suggestionsMap) {
            List<SuggestionClass> suggestionClasses = suggestionClassPermutation.suggestionClasses;
            int size = suggestionClasses.size();
            if (size >= 3) {
                for (int i = 0; i < size - 2; i++) {
                    SuggestionClass first = suggestionClasses.get(i);
                    SuggestionClass second = suggestionClasses.get(i + 1);
                    SuggestionClass third = suggestionClasses.get(i + 2);
                    if (first.entityClass.equals(third.entityClass) && !first.entityClass.equals(second.entityClass)) {
                        // we reduce the weight of all 3 suggestion classes by half
                        first.halfWeightMultiplier();
                        second.halfWeightMultiplier();
                        third.halfWeightMultiplier();
                    }
                }
            }

            suggestionClassPermutation.computeWeight();
        }

    }

    private static class PrioritiseWithBigram implements FeatureFunction {

        @Override
        public void apply(SuggestionClassPermutation suggestionClassPermutation, String[] tokens, Map<String, SuggestionSet> suggestionsMap) {
            // if previous suggestion and this suggestion class are same then we mark it
            suggestionClassPermutation
                    .suggestionClasses
                    .stream()
                    .filter(suggestionClass -> suggestionClass != null)
                    .filter(suggestionClass -> suggestionClass.tokenEnd > suggestionClass.tokenStart)
                    .forEach(suggestionClass -> {
                        String[] previousTokens = new String[2];
                        previousTokens[1] = tokens[suggestionClass.tokenStart];

                        String entityClass = suggestionClass.entityClass;
                        List<SuggestionPermutation> newPermutations = new ArrayList<>();
                        Collection<SuggestionPermutation> existingPermutations = suggestionClass.suggestionPermutations;
                        int suggestionIndex = 1;
                        for (int i = suggestionClass.tokenStart + 1; i <= suggestionClass.tokenEnd; i++) {
                            String currentToken = tokens[i];

                            handleBigram(suggestionsMap, previousTokens, currentToken, newPermutations, existingPermutations, suggestionIndex, entityClass);
                            handleTrigram(suggestionsMap, previousTokens, currentToken, newPermutations, existingPermutations, suggestionIndex, entityClass);

                            previousTokens[0] = previousTokens[1];
                            previousTokens[1] = currentToken;
                            suggestionIndex++;
                        }

                        if (newPermutations.size() > 0) {
                            existingPermutations.addAll(newPermutations);
                            suggestionClass.computeWeight();
                        }
                    });

            suggestionClassPermutation.computeWeight();
        }

        private void handleTrigram(Map<String, SuggestionSet> suggestionsMap,
                                   String[] previousTokens,
                                   String currentToken,
                                   Collection<SuggestionPermutation> newPermutations,
                                   Collection<SuggestionPermutation> existingPermutations,
                                   int suggestionIndex,
                                   String entityClass) {
            // check for trigram
            if (previousTokens[0] != null) {
                String trigramKey = previousTokens[0] + "+" + previousTokens[1] + currentToken;
                addNGramPermutation(suggestionsMap, newPermutations, existingPermutations, suggestionIndex, entityClass, trigramKey, 3);
            }
        }

        private void handleBigram(Map<String, SuggestionSet> suggestionsMap,
                                  String[] previousTokens,
                                  String currentToken,
                                  Collection<SuggestionPermutation> newPermutations,
                                  Collection<SuggestionPermutation> existingPermutations,
                                  int suggestionIndex,
                                  String entityClass) {
            String bigramKey = previousTokens[1] + "+" + currentToken;

            addNGramPermutation(suggestionsMap, newPermutations, existingPermutations, suggestionIndex, entityClass, bigramKey, 2);
        }

        private void addNGramPermutation(Map<String, SuggestionSet> suggestionsMap,
                                         Collection<SuggestionPermutation> newPermutations,
                                         Collection<SuggestionPermutation> existingPermutations,
                                         int suggestionIndex,
                                         String entityClass,
                                         String ngramKey,
                                         int ngramSize) {
            SuggestionSet suggestionSet = suggestionsMap.get(ngramKey);
            if (suggestionSet == null || suggestionSet.getSuggestions() == null || suggestionSet.getSuggestions().length == 0) {
                return;
            }

            Arrays.stream(suggestionSet.getSuggestions())
                    .filter(suggestion -> StringUtils.equals(suggestion.getEntityClass(), entityClass))
                    .forEach(suggestion -> {
                        // generate a new permutation
                        existingPermutations.forEach(existingPermutation -> {
                            List<Suggestion> suggestionList = new ArrayList<>();
                            suggestionList.addAll(existingPermutation.suggestions.subList(0, suggestionIndex - (ngramSize - 1)));
                            suggestionList.add(suggestion);
                            suggestionList.addAll(existingPermutation.suggestions.subList(suggestionIndex + (ngramSize - 1), existingPermutation.suggestions.size()));

                            SuggestionPermutation newPermutation = new SuggestionPermutation(entityClass, suggestionList, existingPermutation.weight * 4.0f);
                            newPermutations.add(newPermutation);
                        });
                    });
        }

    }
}

package io.threesixtyfy.humaneDiscovery.core.tagger;

import io.threesixtyfy.humaneDiscovery.core.conjuncts.TokensBuilder;
import io.threesixtyfy.humaneDiscovery.core.instance.InstanceContext;
import io.threesixtyfy.humaneDiscovery.core.tag.BaseTag;
import io.threesixtyfy.humaneDiscovery.core.tag.TagUtils;
import io.threesixtyfy.humaneDiscovery.core.tagForest.ForestMember;
import io.threesixtyfy.humaneDiscovery.core.tagForest.MatchLevel;
import io.threesixtyfy.humaneDiscovery.core.tagForest.MatchSet;
import io.threesixtyfy.humaneDiscovery.core.tagForest.TagForest;
import io.threesixtyfy.humaneDiscovery.core.tagForest.TokenMatch;
import io.threesixtyfy.humaneDiscovery.core.utils.EditDistanceUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.ScriptSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static io.threesixtyfy.humaneDiscovery.core.tokenIndex.TokenIndexConstants.Fields.KEY;
import static io.threesixtyfy.humaneDiscovery.core.tokenIndex.TokenIndexConstants.Fields.TAGS;
import static io.threesixtyfy.humaneDiscovery.core.tokenIndex.TokenIndexConstants.Fields.TOKENS;

public class TagBuilder {

    private static final Logger logger = Loggers.getLogger(TagBuilder.class);

    private static final TagBuilder INSTANCE = new TagBuilder();
    private static final float EXACT_MATCH_SCORE = 4.0f;
    private static final float EDGE_GRAM_MATCH_SCORE = 2.0f;
    private static final String[] FETCH_SOURCES = new String[]{
            KEY,
            TOKENS,
            TAGS
    };
    private static final String EXPRESSION_LANG = null;
    private static final String INPUT_TOKENS_PARAM = "inputTokens";
    private static final String SORT_SCRIPT = "doc['tokenCount'].value";
    private static final String SCORE_FIELD = "_score";
    private static final int QUERY_TIMEOUT = 400;
    private static final int RESULT_SIZE = 20;
    private final TokenSetQueryBuilder tokenSetQueryBuilder = new TokenSetQueryBuilder();
    private final TokensBuilder tokensBuilder = TokensBuilder.INSTANCE();

    private TagBuilder() {
    }

    public static TagBuilder INSTANCE() {
        return INSTANCE;
    }

    public List<TagForest> tag(InstanceContext instanceContext, AnalysisService analysisService, Client client, String query) {
        long startTime = System.currentTimeMillis();

        List<String> tokensList = tokensBuilder.tokens(analysisService, query);

        int numTokens = tokensList == null ? 0 : tokensList.size();

        if (numTokens == 0 || numTokens > 8) {
            return null;
        }

        List<TagForest> tagForests = tag(instanceContext, client, tokensList);

        if (tagForests == null) {
            return null;
        }

        // normalise score here
        normaliseScore(tagForests);

        // sort
        Collections.sort(tagForests);

        // filter beyond a threshold

        if (logger.isDebugEnabled()) {
            logger.debug("For query: {} and instance: {} built tags in {}ms = {}", query, instanceContext.getName(), (System.currentTimeMillis() - startTime), tagForests);
        }

        return tagForests;
    }

    private void normaliseScore(List<TagForest> tagForests) {
        double max = 0.0f;
        for (TagForest tagForest : tagForests) {
            max = Math.max(max, tagForest.getScore());
        }

        if (max == 0.0f) {
            return;
        }

        double sum = 0.0f;
        for (TagForest tagForest : tagForests) {
            tagForest.setNormalisedScore(Math.exp(tagForest.getScore() * 10.0f / max));
            sum += tagForest.getNormalisedScore();
        }

        if (sum > 0) {
            for (TagForest tagForest : tagForests) {
                tagForest.setNormalisedScore(tagForest.getNormalisedScore() / sum);
            }
        }
    }

    private List<TagForest> tag(InstanceContext instanceContext, Client client, List<String> tokens) {
        if (tokens == null || tokens.size() == 0) {
            return null;
        }

        List<TagForest> tagForests = new ArrayList<>();
        if (tokens.size() == 1) {
            SearchHit[] searchHits = matchTokens(instanceContext, client, tokens);

            if (searchHits == null) {
                return null;
            }

            buildTagForests(instanceContext, tokens, searchHits, tagForests);
        } else {
            List<List<String>> conjuncts = conjuncts(tokens);

            List<SearchHit[]> searchHitsList = matchConjuncts(instanceContext, client, conjuncts);

            Iterator<List<String>> conjuctsIterator = conjuncts.iterator();
            Iterator<SearchHit[]> searchHitsIterator = searchHitsList.iterator();

            while (conjuctsIterator.hasNext() && searchHitsIterator.hasNext()) {
                List<String> conjunct = conjuctsIterator.next();
                SearchHit[] searchHits = searchHitsIterator.next();

                if (searchHits != null) {
                    buildTagForests(instanceContext, conjunct, searchHits, tagForests);
                }
            }
        }

        return tagForests;
    }

    private List<List<String>> conjuncts(List<String> tokens) {
        List<List<String>> conjuncts = new ArrayList<>();

        addIfValidTokens(tokens, conjuncts);

        int size = tokens.size();

        if (size > 1) {
            for (int i = 0; i < size; i++) {
                // 1 size list
                addIfValidTokens(tokens.subList(i, i + 1), conjuncts);

                if (size > 2 && i < size - 1) {
                    // 2 size list
                    addIfValidTokens(tokens.subList(i, i + 2), conjuncts);
                }

                if (size > 3 && i < size - 2) {
                    // 3 size list
                    addIfValidTokens(tokens.subList(i, i + 3), conjuncts);
                }
            }
        }

        return conjuncts;
    }

    private void addIfValidTokens(List<String> tokens, List<List<String>> listOfList) {
        boolean valid = false;

        int size = tokens.size();

        for (String token : tokens) {
            if (!NumberUtils.isParsable(token) && (size > 1 || token.length() > 1)) {
                valid = true;
            }
        }

        if (valid) {
            listOfList.add(tokens);
        }
    }

    private SearchHit[] matchTokens(InstanceContext instanceContext, Client client, List<String> tokens) {
        QueryBuilder queryBuilder = tokenSetQueryBuilder.buildQuery(instanceContext.getTagWeights(), tokens);

        Script sortScript = new Script(SORT_SCRIPT,
                ScriptService.ScriptType.INLINE,
                EXPRESSION_LANG,
                Collections.singletonMap(INPUT_TOKENS_PARAM, tokens.size()));

        org.elasticsearch.action.search.SearchRequestBuilder searchRequestBuilder = client.prepareSearch(instanceContext.getTagIndex())
                .setSize(RESULT_SIZE)
                .setQuery(queryBuilder)
                .addSort(SCORE_FIELD, SortOrder.DESC)
                .addSort(SortBuilders.scriptSort(sortScript, ScriptSortBuilder.ScriptSortType.NUMBER))
                .setFetchSource(FETCH_SOURCES, null);

        if (logger.isDebugEnabled()) {
            logger.debug("For tokens: {} Search Request: {}", tokens, searchRequestBuilder);
        }

        SearchResponse searchResponse = searchRequestBuilder
                .execute()
                .actionGet(QUERY_TIMEOUT, TimeUnit.MILLISECONDS);

        if (searchResponse != null && searchResponse.getHits() != null && searchResponse.getHits().getHits() != null && searchResponse.getHits().getHits().length > 0) {
//            final float maxScore = searchResponse.getHits().getHits()[0].score();

            SearchHit[] searchHits = searchResponse.getHits().getHits();
//            Arrays.stream(searchResponse.getHits().getHits())
//                    .filter(searchHit -> searchHit.getScore() >= (0.375 * maxScore))
//                    .toArray(SearchHit[]::new);

            if (logger.isDebugEnabled()) {
                logger.debug("For tokens {} search hits {}", tokens, searchHits.length);
            }

            for (SearchHit searchHit : searchHits) {
                if (logger.isDebugEnabled()) {
                    logger.debug("[{}] ==> {}", searchHit.getScore(), searchHit.getSource());
                }
            }

            return searchHits;
        }

        return null;
    }

    // TODO: would multi-search be more optimised here
    // TODO: use caching here
    // TODO: use lazy multi-search
    private List<SearchHit[]> matchConjuncts(InstanceContext instanceContext, Client client, List<List<String>> conjuncts) {

        List<SearchHit[]> searchHitsList = new ArrayList<>();

        for (List<String> tokens : conjuncts) {
            searchHitsList.add(matchTokens(instanceContext, client, tokens));
        }

        return searchHitsList;
    }

    // can we optimise the implementation for single tokens
    private void buildTagForests(InstanceContext instanceContext, List<String> tokens, SearchHit[] searchHits, List<TagForest> tagForests) {
        // for all results
        // for each existing buildTagForests
        // find which all tokens it covers
        // if covers ==> permute into a new buildTagForests
        if (searchHits == null) {
            return;
        }

        for (SearchHit searchHit : searchHits) {
            MatchSet brokenTokenMatchSet = buildTokenMatches(instanceContext, tokens, searchHit);

            if (logger.isDebugEnabled()) {
                logger.debug("Input Tokens: {} and Search Hit: {} Token Wise Match Set: {}", tokens, searchHit, brokenTokenMatchSet);
            }

            MatchSet joinedTokenMatchSet = buildJoinedTokenMatches(instanceContext, tokens, searchHit);

            if (logger.isDebugEnabled()) {
                logger.debug("Input Tokens: {} and Search Hit: {} Joined Match Set: {}", tokens, searchHit, joinedTokenMatchSet);
            }

            MatchSet matchSet = bestMatchSet(brokenTokenMatchSet, joinedTokenMatchSet);

            if (logger.isDebugEnabled()) {
                logger.debug("Best MatchSet: {}", matchSet);
            }

            if (matchSet == null) {
                continue;
            }

            // the algorithm:
            // =============
            // check if there is any tagForest (some ForestNode or TagGraph) which fully containsInput current matched tokens
            // or if current matched token fully containsInput some ForestNode or TagGraph of the tagForest
            // if not, then we clone duplicate all existing tagForests and set current matched token to it

            // hyundai => verna => hyundai+verna
            // hyundai => hyundai+verna => verna
            // hyundai+verna => hyundai => verna

            // joined token, lower score
            boolean found = false;
            for (TagForest tagForest : tagForests) {
                // if buildTagForests forest (any ForestNode or TagGraph) containsInput all matched tokens
                // => we don't do anything
                for (ForestMember forestMember : tagForest.getMembers()) {
                    // check whether same input
                    // if same input, do we have better scored one... if yes, replace
                    found = forestMember.containsInput(matchSet);

                    // is match fully contained too...
                    // if not, then we check for scores
                    if (found) {
                        // if same match set too, then we simply merge tags
                        if (forestMember.inputContainedBy(matchSet) && forestMember.containsMatched(matchSet) && forestMember.matchContainedBy(matchSet)) {
                            // do we need to merge tags here
                            forestMember.mergeTags(matchSet);

                            if (logger.isDebugEnabled()) {
                                logger.debug("After merging tags {}", forestMember);
                            }
                        } else if (!forestMember.containsMatched(matchSet) && forestMember.compareTo(matchSet) > 0) {
                            // we replace here
                            if (logger.isDebugEnabled()) {
                                logger.debug("Replacing Forest Member {} with better matchSet {}", forestMember, matchSet);
                            }
                            tagForest.replace(forestMember, matchSet);
                        } else {
                            if (logger.isDebugEnabled()) {
                                logger.debug("TagForest {} already contains better matchSet {} --> not adding", forestMember, matchSet);
                            }
                        }

                        break;
                    }
                }

                // else matched tokens fully contain node in buildTagForests forest (ForestNode or TagGraph)
                // => we replace them together into TagGraph
                if (!found) {
                    List<ForestMember> graphCandidates = null;
                    for (ForestMember forestMember : tagForest.getMembers()) {
                        // this match set is better in a sense - it covers existing forest member and their matches
                        if (forestMember.inputContainedBy(matchSet) && forestMember.matchContainedBy(matchSet)) {
                            if (graphCandidates == null) {
                                graphCandidates = new ArrayList<>();
                            }

                            graphCandidates.add(forestMember);
                        }
                    }

                    if (graphCandidates != null) {
                        // we replace them together
                        found = true;

                        // check if combined score of graphCandidates is less than matchSet
                        if (matchSetBetterThanForestMembers(matchSet, graphCandidates)) {
                            if (logger.isDebugEnabled()) {
                                logger.debug("Replacing {} with matchSet {}", graphCandidates, matchSet);
                            }

                            tagForest.replace(graphCandidates, matchSet);
                        }
                    }
                }

                if (found) {
                    break;
                }
            }

            if (!found) {
                // are there any buildTagForests forest that has same input as matched token
                // yes, we selectively clone them and replace those
                // no, we clone existing and add this to them
                if (tagForests.size() == 0) {
                    // simply add current matched token to a new buildTagForests forest
                    if (logger.isDebugEnabled()) {
                        logger.debug("Adding matchSet {}", matchSet);
                    }

                    tagForests.add(new TagForest().addMember(matchSet));
                } else {
                    // clone existing and replace / add current matched token
                    List<TagForest> newForests = new ArrayList<>();
                    for (TagForest tagForest : tagForests) {
                        List<ForestMember> intersectedMembers = tagForest.intersection(matchSet);
                        if (intersectedMembers == null) {
                            if (logger.isDebugEnabled()) {
                                logger.debug("Appending matchSet {}", matchSet);
                            }

                            tagForest.addMember(matchSet);
                        } else {
                            if (logger.isDebugEnabled()) {
                                logger.debug("Upserting matchSet {}", matchSet);
                            }

                            TagForest clonedTagForest = new TagForest(tagForest.getMembers());
                            newForests.add(clonedTagForest.upsertMember(matchSet));
                        }
                    }

                    tagForests.addAll(newForests);
                }

            }
        }

        if (logger.isDebugEnabled()) {
            logger.debug("For tokens: {}, Tag Forests: {}", tokens, tagForests);
        }
    }

    private TokenMatch exactMatch(List<String> inputTokens, String resultToken) {
        // try finding as full
        for (String token : inputTokens) {
            // does resultToken match some exact 'token' in input
            if (StringUtils.equals(resultToken, token)) {
                return new TokenMatch(token, resultToken, MatchLevel.Exact, EXACT_MATCH_SCORE);
            }
        }

        return null;
    }

    private TokenMatch edgeGramMatch(List<String> inputTokens, String resultToken) {
        // try finding as full
        for (String token : inputTokens) {
            // does resultToken match some exact 'token' in input
            if (StringUtils.startsWith(resultToken, token)) {
                float scoreNormaliser = resultToken.length() - token.length() + 1;
                return new TokenMatch(token, token, MatchLevel.EdgeGram, EDGE_GRAM_MATCH_SCORE / scoreNormaliser);
            }
        }

        return null;
    }

    private TokenMatch phoneticMatch(List<String> inputTokens, String resultToken, boolean noEdgeGramMatch, float phoneticThreshold) {
        String bestInputToken = null;
        String bestMatchedToken = null;
        float bestScore = 0.0f;

        int resultTokenLength = resultToken.length();

        boolean edgeGram = false;
        for (String inputToken : inputTokens) {
            // is there a match
            int inputTokenLength = inputToken.length();

            if (!noEdgeGramMatch && inputTokenLength < resultTokenLength && inputTokenLength >= 3) {
                for (int i = Math.max(3, Math.min(3, inputTokenLength - 1)); i <= resultTokenLength; i++) {
                    String newResultToken = resultToken.substring(0, i);
                    int newResultTokenLength = newResultToken.length();

//                    int totalLength = (newResultTokenLength + inputTokenLength) / 2;

                    float dmDistance = Math.round(200.0f * EditDistanceUtils.getDamerauLevenshteinDistance(newResultToken, inputToken) / (newResultTokenLength + inputTokenLength));

                    if (logger.isDebugEnabled()) {
                        logger.debug("Fuzzy match of {}/{} and {} --> dm dist={}", newResultToken, resultToken, inputToken, dmDistance);
                    }

                    // find better of the lot
                    if (newResultTokenLength < resultTokenLength && dmDistance <= (0.80 * phoneticThreshold) || newResultTokenLength == resultTokenLength && dmDistance <= phoneticThreshold) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Fuzzy matched {} and {}", resultToken, inputToken);
                        }

                        float scoreNormaliser = (resultTokenLength - newResultTokenLength) + 1;

                        float score = (1.0f - (dmDistance / 100.0f)) / scoreNormaliser;
                        if (score <= 0.0f || bestScore < score) {
                            bestScore = score;
                            bestInputToken = inputToken;
                            bestMatchedToken = newResultToken;
                            edgeGram = newResultTokenLength < resultTokenLength;
                        }
                    }
                }
            } else {
//                int totalLength = (resultTokenLength + inputTokenLength) / 2;

                float dmDistance = Math.round(200.0f * EditDistanceUtils.getDamerauLevenshteinDistance(resultToken, inputToken) / (resultTokenLength + inputTokenLength));

                if (logger.isDebugEnabled()) {
                    logger.debug("Fuzzy match of {} and {} --> dm dist={}", resultToken, inputToken, dmDistance);
                }

                // find better of the lot
                if (/*totalLength <= 4 && dmDistance < 40 || totalLength > 4 && */dmDistance <= phoneticThreshold) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Fuzzy matched {} and {}", resultToken, inputToken);
                    }

                    float score = 1.0f - (dmDistance / 100.0f);
                    if (score <= 0.0f || bestScore < score) {
                        bestScore = score;
                        bestMatchedToken = resultToken;
                        bestInputToken = inputToken;
                    }
                }
            }
        }

        if (bestInputToken != null) {
            return new TokenMatch(bestInputToken, bestMatchedToken, edgeGram ? MatchLevel.EdgeGramPhonetic : MatchLevel.Phonetic, bestScore);
        }

        return null;
    }

    private TokenMatch matchedToken(List<String> tokens, String resultToken, boolean noEdgeGramMatch, float phoneticThreshold) {
        TokenMatch tokenMatch = exactMatch(tokens, resultToken);

        if (tokenMatch == null) {
            tokenMatch = edgeGramMatch(tokens, resultToken);
        }

        if (tokenMatch == null) {
            tokenMatch = phoneticMatch(tokens, resultToken, noEdgeGramMatch, phoneticThreshold);
        }

        if (tokenMatch == null) {
            tokenMatch = phoneticMatch(tokens, resultToken, noEdgeGramMatch, phoneticThreshold * 1.20f);
        }

        if (tokenMatch == null) {
            tokenMatch = phoneticMatch(tokens, resultToken, noEdgeGramMatch, phoneticThreshold * 1.40f);
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Input Tokens: {} and Result Token: {} Matched Token: {}", tokens, resultToken, tokenMatch);
        }

        return tokenMatch;
    }

    // TODO: if some input token has already been matched with a resultToken at higher match level, then do not proceed to compare at lower match level
    private MatchSet buildTokenMatches(InstanceContext instanceContext, List<String> tokens, SearchHit searchHit) {
        List<String> resultTokens = SearchHitUtils.fieldValue(searchHit, TOKENS);
        List<Map<String, Object>> tagData = SearchHitUtils.fieldValue(searchHit, TAGS);

        // linked map of input token to matched token, it's score, matched stats
        List<TokenMatch> matches = null;

        for (String resultToken : resultTokens) {
            TokenMatch tokenMatch = matchedToken(tokens, resultToken, false, 50);

            if (tokenMatch != null) {
                if (matches == null) {
                    matches = new ArrayList<>();
                }

                int sameIndex = 0;
                TokenMatch same = null;
                for (TokenMatch match : matches) {
                    if (StringUtils.equals(match.getInputToken(), tokenMatch.getInputToken()) || StringUtils.equals(match.getMatchedToken(), tokenMatch.getMatchedToken())) {
                        same = match;
                        break;
                    }

                    sameIndex++;
                }

                if (same != null) {
                    TokenMatch best = ObjectUtils.min(same, tokenMatch);
                    if (best != same) {
                        // we replace same with tokenMatch
                        matches.remove(sameIndex);
                        matches.add(tokenMatch);
                    }
                } else {
                    matches.add(tokenMatch);
                }
            }
        }

        if (matches == null) {
            return null;
        }

        SortedSet<BaseTag> tags = new TreeSet<>();
        Consumer<BaseTag> consumer = tags::add;

        TagUtils.unmap(tagData, consumer, instanceContext.getTagWeightsMap(), matches.size() < resultTokens.size());

        int totalResultTokens = resultTokens.size();
        float score = 1.0f; //(float) Math.log(searchHit.getScore());
        int matchCode = 0;

        List<String> inputTokens = new ArrayList<>();
//        List<String> matchedTokens = new ArrayList<>();
        for (TokenMatch match : matches) {
            score *= match.getScore();
            matchCode = Math.max(matchCode, match.getMatchLevel().getCode());
            inputTokens.add(match.getInputToken());
//            matchedTokens.add(match.getMatchedToken());
        }

//        score = score * size / totalResultTokens;

        float weight = 0f;
        if (tags.size() > 0 && tags.first().getWeight() != 0) {
            weight = tags.first().getWeight();
        }

        return new MatchSet(inputTokens, matches, MatchLevel.byCode(matchCode), score, weight, tags, totalResultTokens);
    }

    private MatchSet buildJoinedTokenMatches(InstanceContext instanceContext, List<String> inputTokens, SearchHit searchHit) {
        List<String> resultTokens = SearchHitUtils.fieldValue(searchHit, TOKENS);
        List<Map<String, Object>> tagData = SearchHitUtils.fieldValue(searchHit, TAGS);

        if (inputTokens.size() <= 1 && resultTokens.size() <= 1) {
            return null;
        }

        // linked map of input token to matched token, it's score, matched stats
        List<TokenMatch> tokenMatches = null;

        String joinedInputToken = inputTokens.stream().collect(Collectors.joining());
        String joinedResultToken = resultTokens.stream().collect(Collectors.joining());

        TokenMatch tokenMatch = matchedToken(Collections.singletonList(joinedInputToken), joinedResultToken, true, 40);

        if (tokenMatch != null) {
            tokenMatches = new ArrayList<>();
            tokenMatches.add(tokenMatch);
        }

        if (tokenMatches == null) {
            return null;
        }

        SortedSet<BaseTag> tags = new TreeSet<>();
        Consumer<BaseTag> consumer = tags::add;

        TagUtils.unmap(tagData, consumer, instanceContext.getTagWeightsMap(), false);

        float score = tokenMatch.getScore() /* * (float) Math.log(searchHit.getScore())*/;

        float weight = 0f;
        if (tags.size() > 0 && tags.first().getWeight() != 0) {
            weight = tags.first().getWeight();
        }

        List<TokenMatch> tokenMatchList = new ArrayList<>();

        for (String resultToken : resultTokens) {
            tokenMatchList.add(new TokenMatch(joinedInputToken, resultToken, tokenMatch.getMatchLevel(), score));
        }

        return new MatchSet(inputTokens, tokenMatchList, tokenMatch.getMatchLevel(), score, weight, tags, resultTokens.size());
    }

    private MatchSet bestMatchSet(MatchSet one, MatchSet two) {
        if (one == null) {
            return two;
        } else if (two == null) {
            return one;
        } else if (one.compareTo(two) <= 0) {
            return one;
        } else {
            return two;
        }
    }

    private boolean matchSetBetterThanForestMembers(MatchSet matchSet, List<ForestMember> forestMembers) {
        int maxLevel = 0;

        float weight = 1.0f;
        float score = 1.0f;
        for (ForestMember forestMember : forestMembers) {
            maxLevel = Math.max(maxLevel, forestMember.getMatchLevel().getLevel());
            score *= forestMember.getScore();
            weight *= forestMember.getWeight();
        }

        float diffPercent = Math.abs(score - matchSet.getScore()) * 100.0f / Math.max(score, matchSet.getScore());

//        logger.info("Forest Diff: {}, weight: {}, score: {}, maxLevel: {}", diffPercent, weight, score, maxLevel);
//        logger.info("MatchSet weight: {}, score: {}, maxLevel: {}", matchSet.getWeight(), matchSet.getScore(), matchSet.getMatchLevel());

        int ret = 0;
        if (diffPercent < 20f) {
            // we compare on weight
            ret = Float.compare(matchSet.getWeight(), weight);
        }

        if (ret == 0) {
            ret = Float.compare(matchSet.getScore(), score);
        }

        if (ret == 0) {
            ret = Integer.compare(maxLevel, matchSet.getMatchLevel().getLevel());
        }

//        logger.info("Ret: {}", ret);

        return ret >= 0;

    }
}

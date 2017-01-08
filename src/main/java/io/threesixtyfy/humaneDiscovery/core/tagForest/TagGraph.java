package io.threesixtyfy.humaneDiscovery.core.tagForest;

import io.threesixtyfy.humaneDiscovery.core.utils.GsonUtils;

import java.util.List;

public class TagGraph extends ForestMember {

    private final List<String> inputTokens;
    private final List<TokenMatch> matchedTokens;

    public TagGraph(MatchSet matchSet) {
        super(matchSet.getMatchLevel(), matchSet.getScore(), matchSet.getWeight(), matchSet.getTags());

        this.inputTokens = matchSet.getInputTokens();
        this.matchedTokens = matchSet.getMatchedTokens();
    }

    public List<String> getInputTokens() {
        return inputTokens;
    }

    public List<TokenMatch> getMatchedTokens() {
        return matchedTokens;
    }

    // matchSet is fully contained by this graph
    @Override
    public boolean containsMatched(MatchSet matchSet) {
        for (TokenMatch matchedTokenInMatchSet : matchSet.getMatchedTokens()) {
            // find any non matching matched token of the match set
            if (!matchedTokens.contains(matchedTokenInMatchSet)) {
                return false;
            }
        }

        return true;
    }

    // matchSet is fully contained by this graph
    @Override
    public boolean containsInput(MatchSet matchSet) {
        for (String inputTokenInMatchSet : matchSet.getInputTokens()) {
            // find any non matching input token of the match set
            if (!inputTokens.contains(inputTokenInMatchSet)) {
                return false;
            }
        }

        return true;
    }

    // this graph is fully contained by match set
    @Override
    public boolean inputContainedBy(MatchSet matchSet) {
        for (String input : inputTokens) {
            // find any non matching input token in this tag graph
            if (!matchSet.getInputTokens().contains(input)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public boolean matchContainedBy(MatchSet matchSet) {
        for (TokenMatch matched : matchedTokens) {
            // find any non matching matched token in this tag graph
            if (!matchSet.getMatchedTokens().contains(matched)) {
                return false;
            }
        }

        return true;
    }

    // match set and this graph have common input tokens
    @Override
    public boolean intersect(MatchSet matchSet) {
        for (String input : inputTokens) {
            if (matchSet.getInputTokens().contains(input)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TagGraph tagGraph = (TagGraph) o;

        return inputTokens.equals(tagGraph.inputTokens) && matchedTokens.equals(tagGraph.matchedTokens);
    }

    @Override
    public int hashCode() {
        int result = inputTokens.hashCode();
        result = 31 * result + matchedTokens.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return GsonUtils.toJson(this);
    }
}

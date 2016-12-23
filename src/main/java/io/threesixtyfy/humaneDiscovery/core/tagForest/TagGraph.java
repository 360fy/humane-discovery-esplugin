package io.threesixtyfy.humaneDiscovery.core.tagForest;

import io.threesixtyfy.humaneDiscovery.core.utils.GsonUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

public class TagGraph extends BaseForestMember {

    private final List<String> inputTokens;
    private final List<String> matchedTokens;

    public TagGraph(MatchSet matchSet) {
        super(matchSet.getMatchLevel(), matchSet.getScore(), matchSet.getTags());

        this.inputTokens = matchSet.getInputTokens();
        this.matchedTokens = matchSet.getMatchedTokens();
    }

    public List<String> getInputTokens() {
        return inputTokens;
    }

    public List<String> getMatchedTokens() {
        return matchedTokens;
    }

    // matchSet is fully contained by this graph
    @Override
    public boolean containsMatched(MatchSet matchSet) {
        for (String matchedTokenInMatchSet : matchSet.getMatchedTokens()) {
            // find any non matching matched token of the match set
            boolean found = false;
            for (String matchedToken : matchedTokens) {
                if (StringUtils.equals(matchedToken, matchedTokenInMatchSet)) {
                    found = true;
                }
            }

            if (!found) {
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
            boolean found = false;
            for (String inputToken : inputTokens) {
                if (StringUtils.equals(inputToken, inputTokenInMatchSet)) {
                    found = true;
                }
            }

            if (!found) {
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
            boolean found = false;
            for (String inputTokenInMatchSet : matchSet.getInputTokens()) {
                if (StringUtils.equals(input, inputTokenInMatchSet)) {
                    found = true;
                }
            }

            if (!found) {
                return false;
            }
        }

        return true;
    }

    @Override
    public boolean matchContainedBy(MatchSet matchSet) {
        for (String matched : matchedTokens) {
            // find any non matching matched token in this tag graph
            boolean found = false;
            for (String matchedTokenInMatchSet : matchSet.getMatchedTokens()) {
                if (StringUtils.equals(matched, matchedTokenInMatchSet)) {
                    found = true;
                }
            }

            if (!found) {
                return false;
            }
        }

        return true;
    }

    // match set and this graph have common input tokens
    @Override
    public boolean intersect(MatchSet matchSet) {
        for (String input : inputTokens) {
            for (String inputTokenInMatchSet : matchSet.getInputTokens()) {
                if (StringUtils.equals(input, inputTokenInMatchSet)) {
                    return true;
                }
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

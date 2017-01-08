package io.threesixtyfy.humaneDiscovery.core.tagForest;

import io.threesixtyfy.humaneDiscovery.core.utils.GsonUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;

// node is a single tag that is either part of TagGraph or exists alone in TagForest
public class TagNode extends ForestMember {

    private static final Logger logger = Loggers.getLogger(TagNode.class);

    private final String inputToken;
    private final TokenMatch matchedToken;

    public TagNode(MatchSet matchSet) {
        super(matchSet.getMatchLevel(), matchSet.getScore(), matchSet.getWeight(), matchSet.getTags());

        this.inputToken = matchSet.getInputTokens().get(0);
        this.matchedToken = matchSet.getMatchedTokens().get(0);
    }

    public String getInputToken() {
        return inputToken;
    }

    public TokenMatch getMatchedToken() {
        return matchedToken;
    }

    // matchSet is fully contained by this graph
    @Override
    public boolean containsInput(MatchSet matchSet) {
        if (matchSet.getInputTokens().size() > 1) {
            return false;
        }

        String inputTokenInMatchSet = matchSet.getInputTokens().get(0);

        return StringUtils.equals(this.inputToken, inputTokenInMatchSet);
    }

    // matchSet is fully contained by this graph
    @Override
    public boolean containsMatched(MatchSet matchSet) {
        if (matchSet.getMatchedTokens().size() > 1) {
            return false;
        }

        TokenMatch matchedTokenInMatchSet = matchSet.getMatchedTokens().get(0);

        return this.matchedToken.equals(matchedTokenInMatchSet);
    }

    // this graph is fully contained by match set
    @Override
    public boolean inputContainedBy(MatchSet matchSet) {
        boolean found = false;
        for (String inputTokenInMatchSet : matchSet.getInputTokens()) {
            if (StringUtils.equals(this.inputToken, inputTokenInMatchSet)) {
                found = true;
                break;
            }
        }

        return found;
    }

    @Override
    public boolean matchContainedBy(MatchSet matchSet) {
        boolean found = false;
        for (TokenMatch matchedTokenInMatchSet : matchSet.getMatchedTokens()) {
            if (this.matchedToken.equals(matchedTokenInMatchSet)) {
                found = true;
                break;
            }
        }

        return found;
    }

    @Override
    public boolean intersect(MatchSet matchSet) {
        for (TokenMatch inputTokenInMatchSet : matchSet.getMatchedTokens()) {
            if (this.inputToken.equals(inputTokenInMatchSet.getInputToken())) {
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

        TagNode tagNode = (TagNode) o;

        return inputToken.equals(tagNode.inputToken) && matchedToken.equals(tagNode.matchedToken);
    }

    @Override
    public int hashCode() {
        int result = inputToken.hashCode();
        result = 31 * result + matchedToken.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return GsonUtils.toJson(this);
    }
}

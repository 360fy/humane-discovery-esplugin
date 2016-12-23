package io.threesixtyfy.humaneDiscovery.core.tagForest;

import io.threesixtyfy.humaneDiscovery.core.utils.GsonUtils;

public class TokenMatch {

    private final String inputToken;
    private final String matchedToken;
    private final MatchLevel matchLevel;
    private final float score;

    public TokenMatch(String inputToken, String matchedToken, MatchLevel matchLevel, float score) {
        this.inputToken = inputToken;
        this.matchedToken = matchedToken;
        this.matchLevel = matchLevel;
        this.score = score;
    }

    public String getInputToken() {
        return inputToken;
    }

    public String getMatchedToken() {
        return matchedToken;
    }

    public MatchLevel getMatchLevel() {
        return matchLevel;
    }

    public float getScore() {
        return score;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TokenMatch that = (TokenMatch) o;

        return inputToken.equals(that.inputToken) && matchedToken.equals(that.matchedToken);
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

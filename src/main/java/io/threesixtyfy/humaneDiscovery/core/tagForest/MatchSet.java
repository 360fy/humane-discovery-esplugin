package io.threesixtyfy.humaneDiscovery.core.tagForest;

import io.threesixtyfy.humaneDiscovery.core.tag.BaseTag;
import io.threesixtyfy.humaneDiscovery.core.utils.GsonUtils;

import java.util.List;

public class MatchSet implements Comparable<MatchSet> {

    private final int size;
    private final List<String> inputTokens;
    private final List<String> matchedTokens;
    private final List<BaseTag> tags;
    private final int totalResultTokens;

    protected final MatchLevel matchLevel;
    protected final float score;

    public MatchSet(List<String> inputTokens, List<String> matchedTokens, MatchLevel matchLevel, float score, List<BaseTag> tags, int totalResultTokens) {
        this.size = matchedTokens.size();
        this.inputTokens = inputTokens;
        this.matchedTokens = matchedTokens;
        this.matchLevel = matchLevel;
        this.score = score;
        this.tags = tags;
        this.totalResultTokens = totalResultTokens;
    }

    public List<String> getInputTokens() {
        return inputTokens;
    }

    public List<String> getMatchedTokens() {
        return matchedTokens;
    }

    public List<BaseTag> getTags() {
        return tags;
    }

    public int getTotalResultTokens() {
        return totalResultTokens;
    }

    public int getSize() {
        return size;
    }

    public MatchLevel getMatchLevel() {
        return matchLevel;
    }

    public float getScore() {
        return score;
    }

    public boolean isGraph() {
        return matchedTokens.size() > 1;
    }

    @Override
    public String toString() {
        return GsonUtils.toJson(this);
    }

    @Override
    public int compareTo(MatchSet o) {
        int ret = Float.compare(o.score, this.score);

        if (ret == 0) {
            ret = Integer.compare(this.matchLevel.getLevel(), o.getMatchLevel().getLevel());
        }

        if (ret == 0) {
            ret = Integer.compare(this.totalResultTokens, o.totalResultTokens);
        }

        return ret;
    }
}

package io.threesixtyfy.humaneDiscovery.core.tagForest;

import io.threesixtyfy.humaneDiscovery.core.tag.BaseTag;
import io.threesixtyfy.humaneDiscovery.core.utils.GsonUtils;

import java.util.Collection;
import java.util.List;

public class MatchSet extends ComparableMatch {

    private final int size;
    private final List<String> inputTokens;
    private final List<String> matchedTokens;
    private final Collection<BaseTag> tags;
    private final int totalResultTokens;

    public MatchSet(List<String> inputTokens, List<String> matchedTokens, MatchLevel matchLevel, float score, float weight, Collection<BaseTag> tags, int totalResultTokens) {
        super(matchLevel, score, weight);
        this.size = matchedTokens.size();
        this.inputTokens = inputTokens;
        this.matchedTokens = matchedTokens;
        this.tags = tags;
        this.totalResultTokens = totalResultTokens;
    }

    public List<String> getInputTokens() {
        return inputTokens;
    }

    public List<String> getMatchedTokens() {
        return matchedTokens;
    }

    public Collection<BaseTag> getTags() {
        return tags;
    }

    public int getTotalResultTokens() {
        return totalResultTokens;
    }

    public int getSize() {
        return size;
    }

    public boolean isGraph() {
        return matchedTokens.size() > 1;
    }

    @Override
    public String toString() {
        return GsonUtils.toJson(this);
    }

    public int compareTo(ComparableMatch o) {

        int ret = super.compareTo(o);

        if (ret == 0 && o instanceof MatchSet) {
            ret = Integer.compare(this.totalResultTokens, ((MatchSet) o).totalResultTokens);
        }

        return ret;
    }
}

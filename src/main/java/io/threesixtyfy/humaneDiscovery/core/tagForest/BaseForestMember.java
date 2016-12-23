package io.threesixtyfy.humaneDiscovery.core.tagForest;

import io.threesixtyfy.humaneDiscovery.core.tag.BaseTag;

import java.util.List;

public abstract class BaseForestMember implements ForestMember {
    protected final MatchLevel matchLevel;
    protected final float score;

    protected final List<BaseTag> tags;

    public BaseForestMember(MatchLevel matchLevel, float score, List<BaseTag> tags) {
        this.matchLevel = matchLevel;
        this.score = score;
        this.tags = tags;
    }

    public MatchLevel getMatchLevel() {
        return matchLevel;
    }

    public float getScore() {
        return score;
    }

    @Override
    public List<BaseTag> getTags() {
        return tags;
    }
}

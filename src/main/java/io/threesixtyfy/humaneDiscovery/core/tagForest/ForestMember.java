package io.threesixtyfy.humaneDiscovery.core.tagForest;

import io.threesixtyfy.humaneDiscovery.core.tag.BaseTag;

import java.util.Collection;

public abstract class ForestMember extends ComparableMatch {

    protected final Collection<BaseTag> tags;

    public ForestMember(MatchLevel matchLevel, float score, float weight, Collection<BaseTag> tags) {
        super(matchLevel, score, weight);
        this.tags = tags;
    }

    public Collection<BaseTag> getTags() {
        return tags;
    }

    public void mergeTags(MatchSet matchSet) {
        for (BaseTag tag : matchSet.getTags()) {
            if (!tags.contains(tag)) {
                tags.add(tag);
            }
        }
    }

    public abstract boolean containsMatched(MatchSet matchSet);

    public abstract boolean containsInput(MatchSet matchSet);

    public abstract boolean inputContainedBy(MatchSet matchSet);

    public abstract boolean matchContainedBy(MatchSet matchSet);

    public abstract boolean intersect(MatchSet matchSet);

}

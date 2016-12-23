package io.threesixtyfy.humaneDiscovery.core.tagForest;

import io.threesixtyfy.humaneDiscovery.core.tag.BaseTag;

import java.util.List;

public interface ForestMember {

    float getScore();

    MatchLevel getMatchLevel();

    boolean containsMatched(MatchSet matchSet);

    boolean containsInput(MatchSet matchSet);

    boolean inputContainedBy(MatchSet matchSet);

    boolean matchContainedBy(MatchSet matchSet);

    List<BaseTag> getTags();

    boolean intersect(MatchSet matchSet);

}

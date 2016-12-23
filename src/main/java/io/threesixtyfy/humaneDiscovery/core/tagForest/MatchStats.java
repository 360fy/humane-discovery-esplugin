package io.threesixtyfy.humaneDiscovery.core.tagForest;

import io.threesixtyfy.humaneDiscovery.core.utils.GsonUtils;

public class MatchStats implements Comparable<MatchStats> {
    final MatchLevel matchLevel;

    final float score;

    public MatchStats(MatchLevel matchLevel, float score) {
        this.matchLevel = matchLevel;
        this.score = score;
    }

    public MatchLevel getMatchLevel() {
        return matchLevel;
    }

    public float getScore() {
        return score;
    }

    @Override
    public String toString() {
        return GsonUtils.toJson(this);
    }

    @Override
    public int compareTo(MatchStats o) {
        int ret = this.matchLevel.level - o.matchLevel.level;

        if (ret == 0) {
            ret = Float.compare(this.score, o.score);
        }

        return ret;
    }
}

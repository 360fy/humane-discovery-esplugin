package io.threesixtyfy.humaneDiscovery.core.tagForest;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;

public abstract class ComparableMatch implements Comparable<ComparableMatch> {

    private static final Logger logger = Loggers.getLogger(ComparableMatch.class);

    protected final MatchLevel matchLevel;
    protected final float score;
    protected final float weight;

    public ComparableMatch(MatchLevel matchLevel, float score, float weight) {
        this.matchLevel = matchLevel;
        this.score = score;
        this.weight = weight;
    }

    public MatchLevel getMatchLevel() {
        return matchLevel;
    }

    public float getScore() {
        return score;
    }

    public float getWeight() {
        return weight;
    }

    @Override
    public String toString() {
        return "{" +
                "matchLevel=" + matchLevel +
                ", score=" + score +
                ", weight=" + weight +
                '}';
    }

    @Override
    public int compareTo(ComparableMatch o) {
//        logger.info("This: {}, o: {}", this, o);

        float diffPercent = Math.abs(o.score - this.score) * 100.0f / Math.max(o.score, this.score);

//        logger.info("Diff: {}", diffPercent);

        int ret = 0;
        if (diffPercent < 20f) {
            // we compare on weight
            ret = Float.compare(o.weight, this.weight);
        }

        if (ret == 0) {
            ret = Float.compare(o.score, this.score);
        }

        if (ret == 0) {
            ret = Integer.compare(this.matchLevel.getLevel(), o.getMatchLevel().getLevel());
        }

//        logger.info("Ret: {}", ret);

        return ret;
    }
}

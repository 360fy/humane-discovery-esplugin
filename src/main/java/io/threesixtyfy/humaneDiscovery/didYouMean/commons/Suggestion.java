package io.threesixtyfy.humaneDiscovery.didYouMean.commons;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

public class Suggestion implements Comparable<Suggestion> {
    private final ESLogger logger = Loggers.getLogger(Suggestion.class);

    private final String suggestion;
    private final String match;
    private final MatchLevel matchLevel;
    private final int editDistance;
    private final float editDistancePercentage;
    private final int similarity;
    private final float similarityPercentage;
    private final int count;

    public Suggestion(String suggestion, String match, MatchLevel matchLevel, int editDistance, float editDistancePercentage, int similarity, float similarityPercentage, int count) {
        this.suggestion = suggestion;
        this.match = match;
        this.matchLevel = matchLevel;
        this.editDistance = editDistance;
        this.editDistancePercentage = editDistancePercentage;
        this.similarity = similarity;
        this.similarityPercentage = similarityPercentage;
        this.count = count;
    }

    public String getSuggestion() {
        return suggestion;
    }

    public String getMatch() {
        return match;
    }

    public MatchLevel getMatchLevel() {
        return matchLevel;
    }

    public int getEditDistance() {
        return editDistance;
    }

    public float getEditDistancePercentage() {
        return editDistancePercentage;
    }

    public int getSimilarity() {
        return similarity;
    }

    public float getSimilarityPercentage() {
        return similarityPercentage;
    }

    public int getCount() {
        return count;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Suggestion that = (Suggestion) o;

        return suggestion.equals(that.suggestion);

    }

    @Override
    public int hashCode() {
        return suggestion.hashCode();
    }

    @Override
    public int compareTo(Suggestion o) {
        // lower edit distance comes first
        int ret = this.editDistance - o.editDistance;

        if (ret == 0) {
            ret = this.matchLevel.level - o.matchLevel.level;
        }

        if (ret == 0) {
            ret = o.similarity - this.similarity;
        }

        if (ret == 0) {
            ret = o.count - this.count;
        }

        if (ret == 0) {
            ret = this.suggestion.compareTo(o.suggestion);
        }

        return ret;
    }

    @Override
    public String toString() {
        return "Suggestion{" +
                "suggestion='" + suggestion + '\'' +
                ", match='" + match + '\'' +
                ", matchLevel=" + matchLevel +
                ", editDistance=" + editDistance +
                ", editDistancePercentage=" + editDistancePercentage +
                ", similarity=" + similarity +
                ", similarityPercentage=" + similarityPercentage +
                ", count=" + count +
                '}';
    }
}

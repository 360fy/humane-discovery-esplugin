package io.threesixtyfy.humaneDiscovery.didYouMean.commons;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

public class Suggestion implements Comparable<Suggestion> {
    private static final ESLogger logger = Loggers.getLogger(Suggestion.class);

    private final TokenType tokenType;
    private final String suggestion;
    private final String match;
    private final String display;
    private final double weight;
    private final int count;

    private final MatchStats matchStats;

    private boolean ignore = false;

    public Suggestion(TokenType tokenType, String suggestion, String match, String display, MatchStats matchStats, double weight, int count) {
        this.tokenType = tokenType;
        this.suggestion = suggestion;
        this.match = match;
        this.display = display;
        this.matchStats = matchStats;
        this.weight = weight;
        this.count = count;
    }

    public TokenType getTokenType() {
        return tokenType;
    }

    public String getSuggestion() {
        return suggestion;
    }

    public String getMatch() {
        return match;
    }

    public String getDisplay() {
        return display;
    }

    public MatchStats getMatchStats() {
        return matchStats;
    }

    public double getWeight() {
        return weight;
    }

    public int getCount() {
        return count;
    }

    public boolean isIgnore() {
        return ignore;
    }

    public void setIgnore(boolean ignore) {
        this.ignore = ignore;
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
        int ret = this.matchStats.compareTo(o.matchStats);

        if (ret == 0) {
            ret = Double.compare(o.weight / Math.max(1, o.count), weight / Math.max(1, count));
        }

        if (ret == 0) {
            ret = Integer.compare(tokenType.getLevel(), o.tokenType.getLevel());
        }

        if (ret == 0) {
            ret = Double.compare(o.weight, weight);
        }

        if (ret == 0) {
            ret = Integer.compare(o.count, count);
        }

        if (ret == 0) {
            ret = this.suggestion.compareTo(o.suggestion);
        }

        return ret;
    }

    @Override
    public String toString() {
        return "{" +
                "tokenType=" + tokenType +
                ", suggestion='" + suggestion + '\'' +
                ", match='" + match + '\'' +
                ", display='" + display + '\'' +
                ", matchStats=" + matchStats +
                ", weight=" + weight +
                ", count=" + count +
                ", ignore=" + ignore +
                '}';
    }

    public enum TokenType {
        Uni(0),
        Bi(1),
        ShingleUni(2),
        ShingleBi(3);

        final int level;

        TokenType(int level) {
            this.level = level;
        }

        public int getLevel() {
            return level;
        }
    }

    public static class MatchStats implements Comparable<MatchStats> {
        MatchLevel matchLevel;
        int editDistance;
        int similarity;
        double jwDistance;
        double lDistance;
        float score;

        public MatchStats(MatchLevel matchLevel, int editDistance, int similarity, double jwDistance, double lDistance, float score) {
            this.matchLevel = matchLevel;
            this.editDistance = editDistance;
            this.similarity = similarity;
            this.jwDistance = jwDistance;
            this.lDistance = lDistance;
            this.score = score;
        }

        public MatchLevel getMatchLevel() {
            return matchLevel;
        }

        public int getEditDistance() {
            return editDistance;
        }

        public int getSimilarity() {
            return similarity;
        }

        public double getJwDistance() {
            return jwDistance;
        }

        public double getlDistance() {
            return lDistance;
        }

        public float getScore() {
            return score;
        }

        @Override
        public String toString() {
            return "{" +
                    "matchLevel=" + matchLevel +
                    ", editDistance=" + editDistance +
                    ", similarity=" + similarity +
                    ", jwDistance=" + jwDistance +
                    ", lDistance=" + lDistance +
                    ", score=" + score +
                    '}';
        }

        @Override
        public int compareTo(MatchStats o) {
            int ret = this.matchLevel.level - o.matchLevel.level;

            // lower edit distance comes first
            if (ret == 0) {
                ret = Integer.compare(this.editDistance, o.editDistance);
            }

            if (ret == 0) {
                ret = Double.compare(o.lDistance, this.lDistance);
            }

            if (ret == 0) {
                ret = Double.compare(o.jwDistance, this.jwDistance);
            }

            if (ret == 0) {
                ret = Integer.compare(o.similarity, this.similarity);
            }

            return ret;
        }
    }
}

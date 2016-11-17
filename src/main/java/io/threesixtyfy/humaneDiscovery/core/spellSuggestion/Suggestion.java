package io.threesixtyfy.humaneDiscovery.core.spellSuggestion;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;

import java.util.List;

public class Suggestion implements Comparable<Suggestion> {
    private static final Logger logger = Loggers.getLogger(Suggestion.class);

    private final TokenType inputTokenType;
    private final TokenType matchTokenType;
    private final String suggestion;
    private final String match;
    private final String display;

    // total count for the suggestion word
    private final int totalCount;
    private final boolean edgeGram;
    private final String entityClass;
    private final List<EntityType> entityTypeStats;
    private MatchStats matchStats;
    private boolean ignore;
    private int count;
    private float occurrence;
    private float weight;

    public Suggestion(TokenType inputTokenType, TokenType matchTokenType, String suggestion, String match, String display, MatchStats matchStats, int totalCount, String entityClass, List<EntityType> entityTypeStats) {
        this.inputTokenType = inputTokenType;
        this.matchTokenType = matchTokenType;
        this.suggestion = suggestion;
        this.match = match;
        this.display = display;
        this.matchStats = matchStats;
        this.totalCount = totalCount;
        this.edgeGram = !StringUtils.equals(suggestion, match);

        this.entityClass = entityClass;

        this.entityTypeStats = entityTypeStats;

        this.entityTypeStats.forEach(v -> {
            this.count += v.count;
            this.occurrence += v.occurrence;
            this.weight = Math.max(this.getWeight(), v.weight);
        });
    }

    public TokenType getInputTokenType() {
        return inputTokenType;
    }

    public TokenType getMatchTokenType() {
        return matchTokenType;
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

    public void setMatchStats(MatchStats matchStats) {
        this.matchStats = matchStats;
    }

    public int getTotalCount() {
        return totalCount;
    }

    public boolean isIgnore() {
        return ignore;
    }

    public void setIgnore(boolean ignore) {
        this.ignore = ignore;
    }

    public boolean isEdgeGram() {
        return edgeGram;
    }

    public String getEntityClass() {
        return entityClass;
    }

    public int getCount() {
        return count;
    }

    public float getOccurrence() {
        return occurrence;
    }

    public float getWeight() {
        return weight;
    }

    public void setWeight(float weight) {
        this.weight = weight;
    }

    public List<EntityType> getEntityTypeStats() {
        return entityTypeStats;
    }

    public String key() {
        return this.entityClass + "/" + this.suggestion;
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

        if (!suggestion.equals(that.suggestion)) {
            return false;
        }
        return entityClass.equals(that.entityClass);

    }

    @Override
    public int hashCode() {
        int result = suggestion.hashCode();
        result = 31 * result + entityClass.hashCode();
        return result;
    }

    @Override
    public int compareTo(Suggestion o) {
        int ret = this.matchStats.compareTo(o.matchStats);

        if (ret == 0) {
            ret = Integer.compare(matchTokenType.getLevel(), o.matchTokenType.getLevel());
        }

        if (ret == 0) {
            ret = Integer.compare(o.totalCount, totalCount);
        }

        if (ret == 0) {
            ret = this.suggestion.compareTo(o.suggestion);
        }

        if (ret == 0) {
            ret = this.entityClass.compareTo(o.entityClass);
        }

        return ret;
    }

    @Override
    public String toString() {
        return "{" +
                "inputTokenType=" + inputTokenType +
                ", matchTokenType=" + matchTokenType +
                ", suggestion='" + suggestion + '\'' +
                ", match='" + match + '\'' +
                ", display='" + display + '\'' +
                ", totalCount=" + totalCount +
                ", edgeGram=" + edgeGram +
                ", matchStats=" + matchStats +
                ", ignore=" + ignore +
                ", entityClass='" + entityClass + '\'' +
                ", count=" + count +
                ", occurrence=" + occurrence +
                ", weight=" + weight +
                ", entityTypeStats=" + entityTypeStats +
                '}';
    }

    public static class MatchStats implements Comparable<MatchStats> {
        final MatchLevel matchLevel;

        final int editDistance;
        final float score;

        public MatchStats(MatchLevel matchLevel, int editDistance, float score) {
            this.matchLevel = matchLevel;
            this.editDistance = editDistance;
            this.score = score;
        }

        public MatchLevel getMatchLevel() {
            return matchLevel;
        }

        public int getEditDistance() {
            return editDistance;
        }

        public float getScore() {
            return score;
        }

        @Override
        public String toString() {
            return "{" +
                    "matchLevel=" + matchLevel +
                    ", editDistance=" + editDistance +
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
                ret = Float.compare(this.score, o.score);
            }

            return ret;
        }
    }

    public static class EntityType {
        private final String entityClass;
        private final String entityType;
        private final int count;
        private final float occurrence;
        private final float weight;

        public EntityType(String entityClass, String entityType, int count, float occurrence, float weight) {
            this.entityClass = entityClass;
            this.entityType = entityType;
            this.count = count;
            this.occurrence = occurrence;
            this.weight = weight;
        }

        public String getEntityClass() {
            return entityClass;
        }

        public String getEntityType() {
            return entityType;
        }

        public int getCount() {
            return count;
        }

        public float getOccurrence() {
            return occurrence;
        }

        public float getWeight() {
            return weight;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            EntityType that = (EntityType) o;

            if (!entityClass.equals(that.entityClass)) {
                return false;
            }
            return entityType.equals(that.entityType);

        }

        @Override
        public int hashCode() {
            int result = entityClass.hashCode();
            result = 31 * result + entityType.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "{" +
                    "entityClass='" + entityClass + '\'' +
                    ", entityType='" + entityType + '\'' +
                    ", count=" + count +
                    ", occurrence=" + occurrence +
                    ", weight=" + weight +
                    '}';
        }
    }
}

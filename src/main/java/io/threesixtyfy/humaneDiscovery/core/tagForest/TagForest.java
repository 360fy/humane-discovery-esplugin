package io.threesixtyfy.humaneDiscovery.core.tagForest;

import io.threesixtyfy.humaneDiscovery.core.utils.GsonUtils;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

// it's forest of TagGraph or TagNode
public class TagForest implements Comparable<TagForest> {
    private static final Logger logger = Loggers.getLogger(TagForest.class);

    private float score = 0.0f;
    private double normalisedScore = 0.0f;

    private final Set<ForestMember> members = new HashSet<>();

    public TagForest() {
    }

    public TagForest(Set<ForestMember> members) {
        members.forEach(this::add);
    }

    public Set<ForestMember> getMembers() {
        return members;
    }

    public TagForest replace(List<ForestMember> members, MatchSet matchSet) {
        // we remove members
        // and create a new member for the matched tokens
        for (ForestMember member : members) {
            this.remove(member);
        }

        return this.addMember(matchSet);
    }

    public TagForest replace(ForestMember member, MatchSet matchSet) {
        // we remove member
        // and create a new member for the matched tokens
        this.remove(member);

        return this.addMember(matchSet);
    }

    public TagForest addMember(MatchSet matchSet) {
        return add(matchSet.isGraph() ? new TagGraph(matchSet) : new TagNode(matchSet));
    }

    private TagForest add(ForestMember forestMember) {
        if (this.members.add(forestMember)) {
            this.score += forestMember.getScore();
        }

        if (logger.isDebugEnabled()) {
            logger.debug("After adding {} score={}, members={}", forestMember, this.score, this.members);
        }

        return this;
    }

    private TagForest remove(ForestMember forestMember) {
        if (this.members.remove(forestMember)) {
            this.score -= forestMember.getScore();
        }

        if (logger.isDebugEnabled()) {
            logger.debug("After removing {} score={}, members={}", forestMember, this.score, this.members);
        }

        return this;
    }

    public TagForest upsertMember(MatchSet matchSet) {
        List<ForestMember> intersectedMembers = this.intersection(matchSet);
        if (intersectedMembers != null) {
            // remove these first
            for (ForestMember member : intersectedMembers) {
                this.remove(member);
            }
        }

        this.addMember(matchSet);

        return this;
    }

    public List<ForestMember> intersection(MatchSet matchSet) {
        List<ForestMember> intersectedMembers = null;

        for (ForestMember member : this.members) {
            if (member.intersect(matchSet)) {
                if (intersectedMembers == null) {
                    intersectedMembers = new ArrayList<>();
                }

                intersectedMembers.add(member);
            }
        }

        return intersectedMembers;
    }

    public float getScore() {
        return score;
    }

    public double getNormalisedScore() {
        return normalisedScore;
    }

    public void setNormalisedScore(double normalisedScore) {
        this.normalisedScore = normalisedScore;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TagForest tagForest = (TagForest) o;

        return members.equals(tagForest.members);
    }

    @Override
    public int hashCode() {
        return members.hashCode();
    }

    @Override
    public String toString() {
        return GsonUtils.toJson(this);
    }

    @Override
    public int compareTo(TagForest o) {
        return Double.compare(o.normalisedScore, this.normalisedScore);
    }
}

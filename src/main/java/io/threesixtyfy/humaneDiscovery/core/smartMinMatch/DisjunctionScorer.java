package io.threesixtyfy.humaneDiscovery.core.smartMinMatch;


import org.apache.lucene.search.DisiPriorityQueue;
import org.apache.lucene.search.DisiWrapper;
import org.apache.lucene.search.DisjunctionDISIApproximation;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

// based on lucene's class
abstract class DisjunctionScorer extends Scorer {

    private final DisiPriorityQueue subScorers;
    private final DisjunctionDISIApproximation approximation;

    protected DisjunctionScorer(Weight weight, List<Scorer> subScorers) {
        super(weight);
        if (subScorers.size() <= 1) {
            throw new IllegalArgumentException("There must be at least 2 subScorers");
        }
        this.subScorers = new DisiPriorityQueue(subScorers.size());
        for (Scorer scorer : subScorers) {
            final DisiWrapper w = new DisiWrapper(scorer);
            this.subScorers.add(w);
        }
        this.approximation = new DisjunctionDISIApproximation(this.subScorers);
    }

    @Override
    public DocIdSetIterator iterator() {
        return approximation;
    }

    @Override
    public final int docID() {
        return subScorers.top().doc;
    }

    private DisiWrapper getSubMatches() throws IOException {
        return subScorers.topList();
    }

    @Override
    public final int freq() throws IOException {
        DisiWrapper subMatches = getSubMatches();
        int freq = 1;
        for (DisiWrapper w = subMatches.next; w != null; w = w.next) {
            freq += 1;
        }
        return freq;
    }

    @Override
    public final float score() throws IOException {
        return score(getSubMatches());
    }

    /**
     * Compute the score for the given linked list of scorers.
     */
    protected abstract float score(DisiWrapper topList) throws IOException;

    @Override
    public final Collection<ChildScorer> getChildren() {
        ArrayList<ChildScorer> children = new ArrayList<>();
        for (DisiWrapper scorer : subScorers) {
            children.add(new ChildScorer(scorer.scorer, "SHOULD"));
        }
        return children;
    }

}

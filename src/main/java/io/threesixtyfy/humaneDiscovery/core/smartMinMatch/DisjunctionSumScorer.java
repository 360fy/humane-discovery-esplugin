package io.threesixtyfy.humaneDiscovery.core.smartMinMatch;


import org.apache.lucene.search.DisiWrapper;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.List;

// based on lucene's class
final class DisjunctionSumScorer extends DisjunctionScorer {
    private final float[] coord;

    /**
     * Construct a <code>DisjunctionScorer</code>.
     *
     * @param weight     The weight to be used.
     * @param subScorers Array of at least two subscorers.
     * @param coord      Table of coordination factors
     */
    DisjunctionSumScorer(Weight weight, List<Scorer> subScorers, float[] coord) {
        super(weight, subScorers);
        this.coord = coord;
    }

    @Override
    protected float score(DisiWrapper topList) throws IOException {
        double score = 0;
        int freq = 1;
        for (DisiWrapper w = topList; w != null; w = w.next) {
            score += w.scorer.score();
            freq += 1;
        }
        return (float) score * coord[freq];
//        throw new IOException("Let's see if this helps us debug");
    }
}

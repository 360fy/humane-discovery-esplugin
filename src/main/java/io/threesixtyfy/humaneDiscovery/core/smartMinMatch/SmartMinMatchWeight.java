package io.threesixtyfy.humaneDiscovery.core.smartMinMatch;


import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.logging.Loggers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

final class SmartMinMatchWeight extends Weight {

    /**
     * The Similarity implementation.
     */
    final Similarity similarity;
    final SmartMinMatchQuery query;
    final ArrayList<Weight> weights;
    final int maxCoord;  // num optional + num required
    final boolean disableCoord;
    final float coords[];
    final int minMatch;
    private final Logger logger = Loggers.getLogger(SmartMinMatchWeight.class);

    SmartMinMatchWeight(SmartMinMatchQuery query, IndexSearcher searcher, boolean disableCoord) throws IOException {
        super(query);
        this.query = query;
        this.similarity = searcher.getSimilarity(true);
        weights = new ArrayList<>();
        int i = 0;
        int maxCoord = 0;
        for (Query subQuery : query) {
            Weight w = searcher.createWeight(subQuery, true);
            weights.add(w);
            maxCoord++;
            i += 1;
        }
        this.maxCoord = maxCoord;

        // precompute coords (0..N, N).
        // set disableCoord when its explicit, scores are not needed, no scoring clauses, or the sim doesn't use it.
        coords = new float[maxCoord + 1];
        Arrays.fill(coords, 1F);
        coords[0] = 0f;
        if (maxCoord > 0 && !disableCoord) {
            // compute coords from the similarity, look for any actual ones.
            boolean seenActualCoord = false;
            for (i = 1; i < coords.length; i++) {
                coords[i] = coord(i, maxCoord);
                seenActualCoord |= (coords[i] != 1F);
            }
            this.disableCoord = !seenActualCoord;
        } else {
            this.disableCoord = true;
        }

        this.minMatch = findMinMatch(searcher);
    }

    @Override
    public void extractTerms(Set<Term> terms) {
        int i = 0;
        for (Query subQuery : query) {
            weights.get(i).extractTerms(terms);
            i++;
        }
    }

    @Override
    public float getValueForNormalization() throws IOException {
        float sum = 0.0f;
        int i = 0;
        for (Query subQuery : query) {
            // call sumOfSquaredWeights for all clauses in case of side effects
            float s = weights.get(i).getValueForNormalization();         // sum sub weights
            // only add to sum for scoring clauses
            sum += s;
            i += 1;
        }

        return sum;
    }

    public float coord(int overlap, int maxOverlap) {
        if (overlap == 0) {
            // special case that there are only non-scoring clauses
            return 0F;
        } else if (maxOverlap == 1) {
            // LUCENE-4300: in most cases of maxOverlap=1, BQ rewrites itself away,
            // so coord() is not applied. But when BQ cannot optimize itself away
            // for a single clause (minNrShouldMatch, prohibited clauses, etc), it's
            // important not to apply coord(1,1) for consistency, it might not be 1.0F
            return 1F;
        } else {
            // common case: use the similarity to compute the coord
            return similarity.coord(overlap, maxOverlap);
        }
    }

    @Override
    public void normalize(float norm, float boost) {
        for (Weight w : weights) {
            // normalize all clauses, (even if non-scoring in case of side affects)
            w.normalize(norm, boost);
        }
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        List<Explanation> subs = new ArrayList<>();
        int coord = 0;
        float sum = 0.0f;
        int matchCount = 0;
        for (Weight w : weights) {
            Explanation e = w.explain(context, doc);
            if (e.isMatch()) {
                subs.add(e);
                sum += e.getValue();
                coord++;

                matchCount++;
            }
        }

        if (matchCount == 0) {
            return Explanation.noMatch("No matching clauses", subs);
        } else {
            // we have a match
            Explanation result = Explanation.match(sum, "sum of:", subs);
            final float coordFactor = disableCoord ? 1.0f : coord(coord, maxCoord);
            if (coordFactor != 1f) {
                result = Explanation.match(sum * coordFactor, "product of:",
                        result, Explanation.match(coordFactor, "coord(" + coord + "/" + maxCoord + ")"));
            }
            return result;
        }
    }

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
        return opt(subScorers(context), disableCoord);
    }

    private List<Scorer> subScorers(LeafReaderContext context) throws IOException {
        List<Scorer> optional = new ArrayList<>();
        for (Weight w : weights) {
            Scorer subScorer = w.scorer(context);
            if (subScorer != null) {
                optional.add(subScorer);
            }
        }

        return optional;
    }

    private float[] coords(int size, boolean disableCoord) {
        float coords[];
        if (disableCoord) {
            // sneaky: when we do a mixed conjunction/disjunction, we need a fake for the disjunction part.
            coords = new float[size + 1];
            Arrays.fill(coords, 1F);
        } else {
            coords = this.coords;
        }

        return coords;
    }

    private int findMinMatch(IndexSearcher indexSearcher) throws IOException {
        int maxFreq = 1;

        for (LeafReaderContext ctx : indexSearcher.getTopReaderContext().leaves()) { // search each subreader
            maxFreq = Math.max(maxFreq, findMinMatch(ctx));
        }

        return maxFreq;
    }

    private int findMinMatch(LeafReaderContext context) throws IOException {
        int maxFreq = 1;

        Scorer scorer = minMatchFinderScorer(context);

        if (scorer != null) {
            Bits acceptDocs = context.reader().getLiveDocs();

            DocIdSetIterator iterator = scorer.iterator();
            for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
                if (acceptDocs == null || acceptDocs.get(doc)) {
                    int freq = scorer.freq();
                    maxFreq = Math.max(maxFreq, freq);
                }
            }
        }

        return maxFreq;
    }

    private Scorer minMatchFinderScorer(LeafReaderContext context) throws IOException {
        List<Scorer> subScorers = subScorers(context);
        if (subScorers == null || subScorers.size() == 0) {
            return null;
        }

        // pure disjunction
        if (subScorers.size() == 1) {
            return subScorers.get(0);
        } else {
            return new DisjunctionSumScorer(this, subScorers, coords(subScorers.size(), true));
        }
    }

    private Scorer opt(List<Scorer> optional, boolean disableCoord) throws IOException {
        if (optional == null || optional.size() == 0) {
            return null;
        }

        if (optional.size() == 1) {
            Scorer opt = optional.get(0);
            if (!disableCoord && maxCoord > 1) {
                return new BoostedScorer(opt, coord(1, maxCoord));
            } else {
                return opt;
            }
        } else {
            float coords[] = coords(optional.size(), disableCoord);

            int minMatch = Math.min(this.minMatch, optional.size());

            if (minMatch > 1) {
                return new MinShouldMatchSumScorer(this, optional, minMatch, coords);
            } else {
                return new DisjunctionSumScorer(this, optional, coords);
            }
        }
    }
}

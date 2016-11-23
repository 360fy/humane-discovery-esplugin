package io.threesixtyfy.humaneDiscovery.core.smartMinMatch;

import org.apache.lucene.search.FilterScorer;
import org.apache.lucene.search.Scorer;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

// based on lucene's class
final class BoostedScorer extends FilterScorer {
    final float boost;

    BoostedScorer(Scorer in, float boost) {
        super(in);
        this.boost = boost;
    }

    @Override
    public float score() throws IOException {
        return in.score() * boost;
    }

    @Override
    public Collection<ChildScorer> getChildren() {
        return Collections.singleton(new ChildScorer(in, "BOOSTED"));
    }
}

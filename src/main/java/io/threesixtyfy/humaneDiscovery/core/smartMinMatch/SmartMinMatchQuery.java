package io.threesixtyfy.humaneDiscovery.core.smartMinMatch;


import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.similarities.Similarity;
import org.elasticsearch.common.logging.Loggers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class SmartMinMatchQuery extends Query implements Iterable<Query> {

    private static int maxSubQueryCount = 1024;
    private final Logger logger = Loggers.getLogger(SmartMinMatchQuery.class);
    private final boolean disableCoord;
    private final List<Query> subQueries;              // used for toString() and getClauses()
    // cached hash code is ok since boolean subQueries are immutable
    private int hashCode;

    private SmartMinMatchQuery(boolean disableCoord, Query[] clauses) {
        this.disableCoord = disableCoord;
        this.subQueries = Collections.unmodifiableList(Arrays.asList(clauses));
    }

    /**
     * Return the maximum number of subQueries permitted, 1024 by default.
     * Attempts to add more than the permitted number of subQueries cause {@link
     * TooManySubQueries} to be thrown.
     *
     * @see #setMaxSubQueryCount(int)
     */
    public static int getMaxSubQueryCount() {
        return maxSubQueryCount;
    }

    /**
     * Set the maximum number of subQueries permitted per SmartMinMatchQuery.
     * Default value is 1024.
     */
    public static void setMaxSubQueryCount(int maxSubQueryCount) {
        if (maxSubQueryCount < 1) {
            throw new IllegalArgumentException("maxSubQueryCount must be >= 1");
        }
        SmartMinMatchQuery.maxSubQueryCount = maxSubQueryCount;
    }

    /**
     * Return whether the coord factor is disabled.
     */
    public boolean isCoordDisabled() {
        return disableCoord;
    }

    /**
     * Return a list of the subQueries of this {@link SmartMinMatchQuery}.
     */
    public List<Query> subQueries() {
        return subQueries;
    }

    /**
     * Returns an iterator on the subQueries in this query. It implements the {@link Iterable} interface to
     * make it possible to do:
     * <pre class="prettyprint">for (BooleanClause clause : booleanQuery) {}</pre>
     */
    @Override
    public final Iterator<Query> iterator() {
        return subQueries.iterator();
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
        SmartMinMatchQuery query = this;
        return new SmartMinMatchWeight(query, searcher, disableCoord);
    }

    /**
     * Prints a user-readable version of this query.
     */
    @Override
    public String toString(String field) {
        StringBuilder buffer = new StringBuilder();
        buffer.append("SMMQ(");

        int i = 0;
        for (Query subQuery : this) {
            if (subQuery instanceof SmartMinMatchQuery) {  // wrap sub-bools in parens
                buffer.append("(");
                buffer.append(subQuery.toString(field));
                buffer.append(")");
            } else {
                buffer.append(subQuery.toString(field));
            }

            if (i != subQueries.size() - 1) {
                buffer.append(" ");
            }
            i += 1;
        }

        buffer.append(")@");
        buffer.append(this.hashCode());

        return buffer.toString();
    }

    /**
     * Compares the specified object with this boolean query for equality.
     * Returns true if and only if the provided object<ul>
     * <li>is also a {@link SmartMinMatchQuery},</li>
     * <li>has the same value of {@link #isCoordDisabled()}</li>
     * <li>has the same {@link Occur#SHOULD} subQueries, regardless of the order</li>
     * <li>has the same {@link Occur#MUST} subQueries, regardless of the order</li>
     * <li>has the same set of {@link Occur#FILTER} subQueries, regardless of the
     * order and regardless of duplicates</li>
     * <li>has the same set of {@link Occur#MUST_NOT} subQueries, regardless of
     * the order and regardless of duplicates</li></ul>
     */
    @Override
    public boolean equals(Object o) {
        return sameClassAs(o) &&
                equalsTo(getClass().cast(o));
    }

    private boolean equalsTo(SmartMinMatchQuery other) {
        return disableCoord == other.disableCoord &&
                subQueries.equals(other.subQueries);
    }

    private int computeHashCode() {
        int hashCode = Objects.hash(disableCoord, subQueries);
        if (hashCode == 0) {
            hashCode = 1;
        }
        return hashCode;
    }

    @Override
    public int hashCode() {
        // no need for synchronization, in the worst case we would just compute the hash several times.
        if (hashCode == 0) {
            hashCode = computeHashCode();
            assert hashCode != 0;
        }
        assert hashCode == computeHashCode();
        return hashCode;
    }

    /**
     * Thrown when an attempt is made to add more than {@link
     * #getMaxSubQueryCount()} subQueries. This typically happens if
     * a PrefixQuery, FuzzyQuery, WildcardQuery, or TermRangeQuery
     * is expanded to many terms during search.
     */
    public static class TooManySubQueries extends RuntimeException {
        public TooManySubQueries() {
            super("maxSubQueryCount is set to " + maxSubQueryCount);
        }
    }

    /**
     * A builder for boolean subQueries.
     */
    public static class Builder {

        private final Logger logger = Loggers.getLogger(SmartMinMatchQuery.Builder.class);
        private final List<Query> subQueries = new ArrayList<>();
        private boolean disableCoord;

        /**
         * Sole constructor.
         */
        public Builder() {
        }

        /**
         * {@link Similarity#coord(int, int)} may be disabled in scoring, as
         * appropriate. For example, this score factor does not make sense for most
         * automatically generated subQueries, like {@link WildcardQuery} and {@link
         * FuzzyQuery}.
         */
        public Builder setDisableCoord(boolean disableCoord) {
            this.disableCoord = disableCoord;
            return this;
        }

        /**
         * Add a new clause to this {@link Builder}. Note that the order in which
         * subQueries are added does not have any impact on matching documents or query
         * performance.
         *
         * @throws TooManySubQueries if the new number of subQueries exceeds the maximum clause number
         */
        public Builder add(Query query) {
            if (subQueries.size() >= maxSubQueryCount) {
                throw new TooManySubQueries();
            }
            subQueries.add(query);
            return this;
        }

        /**
         * Create a new {@link SmartMinMatchQuery} based on the parameters that have
         * been set on this builder.
         */
        public SmartMinMatchQuery build() {
            return new SmartMinMatchQuery(disableCoord, subQueries.toArray(new Query[subQueries.size()]));
        }

    }

}

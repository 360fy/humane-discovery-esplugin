package io.threesixtyfy.humaneDiscovery.didYouMean.commons;

import java.util.ArrayList;
import java.util.List;

public class Disjunct {
    final String key;
    final Conjunct[] conjuncts;

    public Disjunct(String key, Conjunct[] conjuncts) {
        this.key = key;
        this.conjuncts = conjuncts;
    }

    public static DisjunctBuilder builder() {
        return new DisjunctBuilder();
    }

    public String getKey() {
        return key;
    }

    public Conjunct[] getConjuncts() {
        return conjuncts;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Disjunct disjunct = (Disjunct) o;

        return key.equals(disjunct.key);

    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }

    @Override
    public String toString() {
        return key/* + " = " + conjuncts*/;
    }

    public static class DisjunctBuilder {
        private StringBuilder disjunctKey = new StringBuilder();
        private List<Conjunct> conjuncts = new ArrayList<>();

        public DisjunctBuilder add(Conjunct conjunct) {
            disjunctKey.append("(").append(conjunct.getKey()).append(")");
            this.conjuncts.add(conjunct);
            return this;
        }

        public Disjunct build() {
            return new Disjunct(this.disjunctKey.toString(), this.conjuncts.toArray(new Conjunct[this.conjuncts.size()]));
        }
    }
}

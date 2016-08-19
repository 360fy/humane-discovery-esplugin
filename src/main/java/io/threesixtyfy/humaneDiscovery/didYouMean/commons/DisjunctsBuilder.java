package io.threesixtyfy.humaneDiscovery.didYouMean.commons;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DisjunctsBuilder {

    // TODO: add key prefix for both disjunct and conjunct... key prefix would typically be the index
    public Disjunct[] build(List<String> tokens, Map<String, Conjunct> uniqueConjuncts) {
        int size = tokens.size();

        if (size == 0) {
            return new Disjunct[0];
        }

        Set<Disjunct> disjuncts = new HashSet<>();

        if (size == 1) {
            disjuncts.add(Disjunct
                    .builder()
                    .add(Conjunct
                            .builder()
                            .add(tokens.get(0))
                            .build(uniqueConjuncts))
                    .build());
        } else {
            if (size <= 3) {
                Conjunct.ConjunctBuilder conjunctBuilder = Conjunct.builder();

                tokens.forEach(conjunctBuilder::add);

                disjuncts.add(Disjunct.builder().add(conjunctBuilder.build(uniqueConjuncts)).build());

                // if any of the token is stop word, then we build another disjunct ignoring that token
                boolean stopWord = false;
                for (String token : tokens) {
                    if (StopWords.contains(token)) {
                        stopWord = true;
                        break;
                    }
                }

                if (stopWord) {
                    conjunctBuilder = Conjunct.builder();

                    for (String token : tokens) {
                        if (!StopWords.contains(token)) {
                            conjunctBuilder.add(token);
                        }
                    }

                    disjuncts.add(Disjunct.builder().add(conjunctBuilder.build(uniqueConjuncts)).build());
                }
            }

            Disjunct[] suffixDisjuncts = build(tokens.subList(1, size), uniqueConjuncts);
            for (Disjunct disjunct : suffixDisjuncts) {
                String prefix = tokens.get(0);

                Disjunct.DisjunctBuilder disjunctBuilder = Disjunct.builder()
                        .add(Conjunct.builder().add(prefix).build(uniqueConjuncts));

                Arrays.stream(disjunct.getConjuncts()).forEach(disjunctBuilder::add);

                disjuncts.add(disjunctBuilder.build());

                // for all disjunct, if first conjunct length is < 3, add this to conjunct to create a new conjunct
                Conjunct firstConjunct = disjunct.getConjuncts()[0];

                if (firstConjunct.getLength() < 3) {
                    disjunctBuilder = Disjunct.builder();

                    firstConjunct = Conjunct.builder().add(prefix, firstConjunct).build(uniqueConjuncts);
                    disjunctBuilder.add(firstConjunct);

                    int conjunctCount = disjunct.getConjuncts().length;

                    for (int i = 1; i < conjunctCount; i++) {
                        Conjunct conjunct = disjunct.getConjuncts()[i];
                        disjunctBuilder.add(conjunct);
                    }

                    disjuncts.add(disjunctBuilder.build());
                }
            }

            Disjunct[] prefixDisjuncts = build(tokens.subList(0, size - 1), uniqueConjuncts);
            for (Disjunct disjunct : prefixDisjuncts) {
                String suffix = tokens.get(size - 1);
                Disjunct.DisjunctBuilder disjunctBuilder = Disjunct.builder();

                Arrays.stream(disjunct.getConjuncts()).forEach(disjunctBuilder::add);

                disjunctBuilder.add(Conjunct.builder().add(suffix).build(uniqueConjuncts));

                disjuncts.add(disjunctBuilder.build());

                // for all disjunct, if last conjunct length is < 3, add this to conjunct to create a new conjunct
                int conjunctCount = disjunct.getConjuncts().length;
                Conjunct lastConjunct = disjunct.getConjuncts()[conjunctCount - 1];
                if (lastConjunct.getLength() < 3) {
                    disjunctBuilder = Disjunct.builder();

                    for (int i = 0; i < conjunctCount - 1; i++) {
                        Conjunct conjunct = disjunct.getConjuncts()[i];
                        disjunctBuilder.add(conjunct);
                    }

                    lastConjunct = Conjunct.builder().add(lastConjunct, suffix).build(uniqueConjuncts);
                    disjunctBuilder.add(lastConjunct);

                    disjuncts.add(disjunctBuilder.build());
                }
            }
        }

        return disjuncts.toArray(new Disjunct[disjuncts.size()]);
    }
}

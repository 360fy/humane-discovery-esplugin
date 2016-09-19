package io.threesixtyfy.humaneDiscovery.core.conjuncts;

import io.threesixtyfy.humaneDiscovery.core.dictionary.StopWords;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DisjunctsBuilder {

    private static final DisjunctsBuilder instance = new DisjunctsBuilder();

    public static DisjunctsBuilder INSTANCE() {
        return instance;
    }

    private DisjunctsBuilder() {
    }

    // TODO: add key prefix for both disjunct and conjunct... key prefix would typically be the index
    public Disjunct[] build(List<String> tokens, Map<String, Conjunct> uniqueConjuncts, int maxLength) {
        return build(0, tokens, uniqueConjuncts, maxLength);
    }

    private Disjunct[] build(int tokenStartIndex, List<String> tokens, Map<String, Conjunct> uniqueConjuncts, int maxLength) {
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
                            .add(tokens.get(0), tokenStartIndex)
                            .build(uniqueConjuncts))
                    .build());
        } else {
            if (size <= maxLength) {
                Conjunct.ConjunctBuilder conjunctBuilder = Conjunct.builder();

                int position = tokenStartIndex;
                for (String token : tokens) {
                    conjunctBuilder.add(token, position);
                    position++;
                }

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

                    position = tokenStartIndex;
                    for (String token : tokens) {
                        if (!StopWords.contains(token)) {
                            conjunctBuilder.add(token, position);
                        }

                        position++;
                    }

                    disjuncts.add(Disjunct.builder().add(conjunctBuilder.build(uniqueConjuncts)).build());
                }
            }

            Disjunct[] suffixDisjuncts = build(tokenStartIndex + 1, tokens.subList(1, size), uniqueConjuncts, maxLength);
            for (Disjunct disjunct : suffixDisjuncts) {
                String prefix = tokens.get(0);

                Disjunct.DisjunctBuilder disjunctBuilder = Disjunct.builder()
                        .add(Conjunct.builder().add(prefix, tokenStartIndex).build(uniqueConjuncts));

                Arrays.stream(disjunct.getConjuncts()).forEach(disjunctBuilder::add);

                disjuncts.add(disjunctBuilder.build());

                // for all disjunct, if first conjunct length is < maxLength, add this to conjunct to create a new conjunct
                Conjunct firstConjunct = disjunct.getConjuncts()[0];

                if (firstConjunct.getLength() < maxLength) {
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

            Disjunct[] prefixDisjuncts = build(tokenStartIndex, tokens.subList(0, size - 1), uniqueConjuncts, maxLength);
            for (Disjunct disjunct : prefixDisjuncts) {
                String suffix = tokens.get(size - 1);
                Disjunct.DisjunctBuilder disjunctBuilder = Disjunct.builder();

                Arrays.stream(disjunct.getConjuncts()).forEach(disjunctBuilder::add);

                disjunctBuilder.add(Conjunct.builder().add(suffix, tokenStartIndex + size - 1).build(uniqueConjuncts));

                disjuncts.add(disjunctBuilder.build());

                // for all disjunct, if last conjunct length is < maxLength, add this to conjunct to create a new conjunct
                int conjunctCount = disjunct.getConjuncts().length;
                Conjunct lastConjunct = disjunct.getConjuncts()[conjunctCount - 1];
                if (lastConjunct.getLength() < maxLength) {
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

package io.threesixtyfy.humaneDiscovery;

import io.threesixtyfy.humaneDiscovery.core.conjuncts.Conjunct;
import io.threesixtyfy.humaneDiscovery.core.conjuncts.Disjunct;
import io.threesixtyfy.humaneDiscovery.core.conjuncts.DisjunctsBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Test {

    public static void main(String[] args) {
        List<String> tokens = new ArrayList<>();
        tokens.add("A");
        tokens.add("B");
        tokens.add("C");
//        tokens.add("D");
//        tokens.add("E");

        DisjunctsBuilder disjunctsBuilder = DisjunctsBuilder.INSTANCE();

        Map<String, Conjunct> uniqueConjuncts = new HashMap<>();
        Disjunct[] disjuncts = disjunctsBuilder.build(tokens, uniqueConjuncts, 3);

        System.out.println(uniqueConjuncts);

        Arrays.asList(disjuncts).forEach(System.out::println);
    }

}

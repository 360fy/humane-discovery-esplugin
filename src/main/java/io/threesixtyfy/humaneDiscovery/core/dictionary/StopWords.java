package io.threesixtyfy.humaneDiscovery.core.dictionary;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class StopWords {

    private static final Set<String> WordList = new HashSet<>(Arrays.asList(
            "a","an", "and", "are", "as", "at", "be", "but", "by",
            "for", "if", "in", "into", "is", "it",
            "no", "not", "of", "on", "or", "such",
            "that", "the", "their", "then", "there", "these",
            "they", "this", "to", "was", "will", "with",
            "tablet", "tablets", "injection", "injections", "syrup",
            "capsule", "capsules"
    ));

    public static boolean contains(String word) {
        return WordList.contains(word);
    }

}

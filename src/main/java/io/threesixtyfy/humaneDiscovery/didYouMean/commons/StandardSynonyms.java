package io.threesixtyfy.humaneDiscovery.didYouMean.commons;

import java.util.HashMap;
import java.util.Map;

public class StandardSynonyms {

    private final Map<String, String[]> synonyms = new HashMap<>();

    public StandardSynonyms() {
        synonyms.put("tablet", new String[]{"tablets"});
        synonyms.put("tablets", new String[]{"tablet"});
        synonyms.put("injection", new String[]{"injections"});
        synonyms.put("injections", new String[]{"injection"});
        synonyms.put("advance", new String[]{"advanced"});
        synonyms.put("advanced", new String[]{"advance"});
        synonyms.put("capsule", new String[]{"capsules"});
        synonyms.put("capsules", new String[]{"capsule"});
        synonyms.put("hilamya", new String[]{"himalaya"});
    }

    public String[] get(String word) {
        return synonyms.get(word);
    }
}

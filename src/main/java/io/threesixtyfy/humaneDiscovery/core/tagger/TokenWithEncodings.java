package io.threesixtyfy.humaneDiscovery.core.tagger;

import java.util.Map;
import java.util.Set;

public class TokenWithEncodings {
    final String token;
    final Map<String, Set<String>> encodings;

    public TokenWithEncodings(String token, Map<String, Set<String>> encodings) {
        this.token = token;
        this.encodings = encodings;
    }
}

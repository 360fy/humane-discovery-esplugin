package io.threesixtyfy.humaneDiscovery.core.tokenIndex;

import io.threesixtyfy.humaneDiscovery.core.utils.GsonUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TokenEncoding {

    private final String token;
    private final Map<String, List<String>> encodings;
    private final double weight;
    private final TokenType tokenType;

    public TokenEncoding(String token, Map<String, List<String>> encodings, TokenType tokenType, double weight) {
        this.token = token;
        this.encodings = encodings;
        this.tokenType = tokenType;
        this.weight = weight;
    }

    public String getToken() {
        return token;
    }

    public Map<String, List<String>> getEncodings() {
        return encodings;
    }

    public TokenType getTokenType() {
        return tokenType;
    }

    public double getWeight() {
        return weight;
    }

    @SuppressWarnings("unchecked")
    public static TokenEncoding unmap(Map<String, Object> map) {
        return new TokenEncoding((String) map.get(TokenIndexConstants.Fields.TOKEN),
                (Map<String, List<String>>) map.get(TokenIndexConstants.Fields.ENCODINGS),
                TokenType.valueOf((String) map.get(TokenIndexConstants.Fields.TOKEN_TYPE)),
                (double) map.get(TokenIndexConstants.Fields.WEIGHT));
    }

    public Map<String, Object> map() {
        Map<String, Object> map = new HashMap<>();

        map.put(TokenIndexConstants.Fields.TOKEN, token);
        map.put(TokenIndexConstants.Fields.ENCODINGS, encodings);
        map.put(TokenIndexConstants.Fields.TOKEN_TYPE, tokenType);
        map.put(TokenIndexConstants.Fields.WEIGHT, weight);

        return map;
    }

    @Override
    public String toString() {
        return GsonUtils.toJson(this);
    }

    public enum TokenType {
        Full,
        EdgeGram,
        Joined
    }
}

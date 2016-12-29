package io.threesixtyfy.humaneDiscovery.core.tokenIndex;

import io.threesixtyfy.humaneDiscovery.core.tag.BaseTag;
import io.threesixtyfy.humaneDiscovery.core.tag.IntentTag;
import io.threesixtyfy.humaneDiscovery.core.tag.KeywordTag;
import io.threesixtyfy.humaneDiscovery.core.tag.NGramTag;
import io.threesixtyfy.humaneDiscovery.core.tag.StopWordTag;
import io.threesixtyfy.humaneDiscovery.core.tag.TagType;
import io.threesixtyfy.humaneDiscovery.core.tag.TagUtils;
import io.threesixtyfy.humaneDiscovery.core.utils.GsonUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class TokenInfo {

    private static final int DEFAULT_COUNT = 1;

    private final String indexName;

    private final String key;
    private final List<String> tokens;

    private List<TokenEncoding> encodings;

    private final Map<String, BaseTag> tags = new HashMap<>();

    private int totalCount = DEFAULT_COUNT;

    public static TokenInfo intentToken(String indexName, List<String> tokens, String intentName, Map<String, List<String>> ancestors) {
        return new TokenInfo(indexName, tokens).add(new IntentTag(intentName, DEFAULT_COUNT, ancestors));
    }

    public static TokenInfo keywordToken(String indexName, List<String> tokens, String keywordName, String normalisedValue) {
        return new TokenInfo(indexName, tokens).add(new KeywordTag(keywordName, normalisedValue));
    }

    public static TokenInfo ngramToken(String indexName, List<String> tokens, String name, Map<String, List<String>> ancestors) {
        return new TokenInfo(indexName, tokens).add(new NGramTag(name, TagType.Intent, DEFAULT_COUNT, ancestors));
    }

    public static TokenInfo stopWordToken(String indexName, List<String> tokens, String stopWordName) {
        return new TokenInfo(indexName, tokens).add(new StopWordTag(stopWordName));
    }

    public TokenInfo(String indexName, String key, List<String> tokens) {
        this.indexName = indexName;
        this.tokens = tokens;
        this.key = key;
    }

    public TokenInfo(String indexName, List<String> tokens) {
        this(indexName, tokens.stream().collect(Collectors.joining("/")), tokens);
    }

    public String getIndexName() {
        return indexName;
    }

    public String getKey() {
        return key;
    }

    public List<String> getTokens() {
        return tokens;
    }

    public List<TokenEncoding> getEncodings() {
        return encodings;
    }

    public void setEncodings(List<TokenEncoding> encodings) {
        this.encodings = encodings;
    }

    public Map<String, BaseTag> getTags() {
        return tags;
    }

    public int getTotalCount() {
        return totalCount;
    }

    @SuppressWarnings("unchecked")
    private static <T> T value(Map<String, Object> map, String key) {
        Object value = map.get(key);
        return value == null ? null : (T) value;
    }

    @SuppressWarnings("unchecked")
    private static <T> T value(Map<String, Object> map, String key, T defaultValue) {
        Object value = map.get(key);
        return value == null ? defaultValue : (T) value;
    }

    @SuppressWarnings("unchecked")
    private static TokenInfo unmap(TokenInfo tokenInfo, Map<String, Object> map) {
        tokenInfo.totalCount = value(map, TokenIndexConstants.Fields.TOTAL_COUNT, DEFAULT_COUNT);

        List<Map<String, Object>> tagDataList = value(map, TokenIndexConstants.Fields.TAGS);

        TagUtils.unmap(tagDataList, tokenInfo::add);

        List<Map<String, Object>> encodingDataList = value(map, TokenIndexConstants.Fields.ENCODINGS);
        if (encodingDataList != null) {
            tokenInfo.encodings = new ArrayList<>(encodingDataList.size());

            for (Map<String, Object> encodingData : encodingDataList) {
                tokenInfo.encodings.add(TokenEncoding.unmap(encodingData));
            }
        }

        return tokenInfo;
    }

    @SuppressWarnings("unchecked")
    public static TokenInfo unmap(String indexName, Map<String, Object> map) {
        return unmap(new TokenInfo(indexName, value(map, TokenIndexConstants.Fields.KEY), value(map, TokenIndexConstants.Fields.TOKENS)), map);
    }

    public Map<String, Object> map() {
        Map<String, Object> map = new HashMap<>();

        map.put(TokenIndexConstants.Fields.KEY, key);
        map.put(TokenIndexConstants.Fields.TOKENS, tokens);
        map.put(TokenIndexConstants.Fields.TOKEN_COUNT, tokens.size());


        if (encodings != null) {
            map.put(TokenIndexConstants.Fields.ENCODINGS, encodings.stream().map(TokenEncoding::map).collect(Collectors.toList()));
        }

        if (tags != null) {
            map.put(TokenIndexConstants.Fields.TAGS, tags.values().stream().map(BaseTag::map).collect(Collectors.toList()));

            Set<String> scopes = tags.values().stream().map(BaseTag::getName).collect(Collectors.toSet());
            map.put(TokenIndexConstants.Fields.SCOPES, scopes.stream().collect(Collectors.toList()));
        }

        if (totalCount > 0) {
            map.put(TokenIndexConstants.Fields.TOTAL_COUNT, totalCount);
        }

        return map;
    }

    public void aggregate(TokenInfo tokenInfo) {
        if (!StringUtils.equals(tokenInfo.key, this.key)) {
            throw new IllegalArgumentException("Trying to aggregate two different keys " + this.key + " and " + tokenInfo.key);
        }

        this.totalCount += tokenInfo.totalCount;

        tokenInfo.tags
                .entrySet()
                .forEach(e -> {
                    BaseTag existingTag = this.tags.get(e.getKey());
                    if (existingTag == null) {
                        this.add(e.getValue());
                    } else {
                        existingTag.merge(e.getValue());
                    }
                });

        tokenInfo.clear();

//        if (this.hasNonNGramTag()) {
//            // remove all ngram tags
//            this.tags.entrySet().removeIf(t -> t.getValue().getTagType() == TagType.NGram);
//        }
    }

    private boolean hasNonNGramTag() {
        for (BaseTag baseTag : this.tags.values()) {
            if (baseTag.getTagType() == TagType.Intent || baseTag.getTagType() == TagType.Keyword || baseTag.getTagType() == TagType.StopWord) {
                return true;
            }
        }

        return false;
    }

    private TokenInfo add(BaseTag tag) {
        this.tags.put(tag.key(), tag);
        return this;
    }

    private void clear() {
        this.tags.values().forEach(BaseTag::clear);
        this.tags.clear();

        if (this.encodings != null) {
            this.encodings.clear();
        }
    }

    @Override
    public String toString() {
        return GsonUtils.toJson(this);
    }
}

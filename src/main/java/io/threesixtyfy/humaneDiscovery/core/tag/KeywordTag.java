package io.threesixtyfy.humaneDiscovery.core.tag;

import io.threesixtyfy.humaneDiscovery.core.utils.GsonUtils;
import io.threesixtyfy.humaneDiscovery.core.tokenIndex.TokenIndexConstants;

import java.util.HashMap;
import java.util.Map;

public class KeywordTag extends BaseTag<KeywordTag> {

    private String normalisedValue;

    public KeywordTag() {
        super(TagType.Keyword);
    }

    public KeywordTag(String name, String normalisedValue) {
        super(TagType.Keyword, name);
        this.normalisedValue = normalisedValue;
    }

    public String getNormalisedValue() {
        return normalisedValue;
    }

    public static KeywordTag unmap(Map<String, Object> map) {
        KeywordTag keywordTag = new KeywordTag().unmapInternal(map);

        keywordTag.normalisedValue = (String) map.get(TokenIndexConstants.Fields.NORMALISED_VALUE);

        return keywordTag;
    }

    public Map<String, Object> map() {
        Map<String, Object> map = this.map(new HashMap<>());

        map.put(TokenIndexConstants.Fields.NORMALISED_VALUE, normalisedValue);

        return map;
    }

    @Override
    public String toString() {
        return GsonUtils.toJson(this);
    }
}

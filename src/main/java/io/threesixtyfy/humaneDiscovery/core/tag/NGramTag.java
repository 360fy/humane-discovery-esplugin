package io.threesixtyfy.humaneDiscovery.core.tag;

import io.threesixtyfy.humaneDiscovery.core.utils.GsonUtils;
import io.threesixtyfy.humaneDiscovery.core.tokenIndex.TokenIndexConstants;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NGramTag extends TagWithCount<NGramTag> {

    private TagType refTagType;

    public NGramTag() {
        super(TagType.NGram);
    }

    public NGramTag(String name, TagType refTagType, int totalCount, List<List<String>> ancestors) {
        super(TagType.NGram, name, totalCount, ancestors);
        this.refTagType = refTagType;
    }

    public TagType getRefTagType() {
        return refTagType;
    }

    public static NGramTag unmap(Map<String, Object> map) {
        NGramTag nGramTag = new NGramTag().unmapInternal(map);
        nGramTag.refTagType = TagType.valueOf((String) map.get(TokenIndexConstants.Fields.REF_TAG_TYPE));

        return nGramTag;
    }

    public Map<String, Object> map() {
        Map<String, Object> map = this.map(new HashMap<>());

        map.put(TokenIndexConstants.Fields.REF_TAG_TYPE, refTagType.name());

        return map;
    }

    @Override
    public String toString() {
        return GsonUtils.toJson(this);
    }
}

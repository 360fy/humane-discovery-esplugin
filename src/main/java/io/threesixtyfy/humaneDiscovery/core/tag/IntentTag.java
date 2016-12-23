package io.threesixtyfy.humaneDiscovery.core.tag;

import io.threesixtyfy.humaneDiscovery.core.utils.GsonUtils;

import java.util.List;
import java.util.Map;

public class IntentTag extends TagWithCount<IntentTag> {

    public IntentTag() {
        super(TagType.Intent);
    }

    public IntentTag(String name, int totalCount, List<List<String>> ancestors) {
        super(TagType.Intent, name, totalCount, ancestors);
    }

    public static IntentTag unmap(Map<String, Object> map) {
        return new IntentTag().unmapInternal(map);
    }

    @Override
    public String toString() {
        return GsonUtils.toJson(this);
    }
}

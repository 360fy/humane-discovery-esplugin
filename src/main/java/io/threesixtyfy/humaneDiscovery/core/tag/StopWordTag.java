package io.threesixtyfy.humaneDiscovery.core.tag;

import io.threesixtyfy.humaneDiscovery.core.utils.GsonUtils;

import java.util.HashMap;
import java.util.Map;

public class StopWordTag extends BaseTag<StopWordTag> {

    public StopWordTag() {
        super(TagType.StopWord);
    }

    public StopWordTag(String name) {
        super(TagType.StopWord, name);
    }

    public static StopWordTag unmap(Map<String, Object> map) {
        return new StopWordTag().unmapInternal(map);
    }

    @Override
    public Map<String, Object> map() {
        return super.map(new HashMap<>());
    }

    @Override
    public String toString() {
        return GsonUtils.toJson(this);
    }
}

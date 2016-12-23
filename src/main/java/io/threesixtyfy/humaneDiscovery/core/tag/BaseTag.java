package io.threesixtyfy.humaneDiscovery.core.tag;

import io.threesixtyfy.humaneDiscovery.core.tokenIndex.TokenIndexConstants;

import java.util.Map;

public abstract class BaseTag<T extends BaseTag<T>> {

    private String key;
    private TagType tagType;
    private String name;

    public BaseTag(TagType tagType) {
        this.tagType = tagType;
    }

    public BaseTag(TagType tagType, String name) {
        this.tagType = tagType;
        this.name = name;
        this.key = this.tagType.name() + "/" + this.name;
    }

    public TagType getTagType() {
        return tagType;
    }

    public String getName() {
        return name;
    }

    public String key() {
        if (this.key == null) {
            this.key = this.tagType.name() + "/" + this.name;
        }

        return this.key;
    }

    @SuppressWarnings("unchecked")
    protected T unmapInternal(Map<String, Object> map) {
        this.tagType = TagType.valueOf((String) map.get(TokenIndexConstants.Fields.TAG_TYPE));
        this.name = (String) map.get(TokenIndexConstants.Fields.NAME);
        this.key = this.tagType.name() + "/" + this.name;

        return (T) this;
    }

    protected Map<String, Object> map(Map<String, Object> map) {
        map.put(TokenIndexConstants.Fields.TAG_TYPE, tagType.name());
        map.put(TokenIndexConstants.Fields.NAME, name);

        return map;
    }

    public abstract Map<String, Object> map();

    @Override
    public String toString() {
        return "{" +
                "tagType=" + tagType +
                ", name='" + name + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BaseTag<?> baseTag = (BaseTag<?>) o;

        return tagType == baseTag.tagType && name.equals(baseTag.name);
    }

    @Override
    public int hashCode() {
        int result = tagType.hashCode();
        result = 31 * result + name.hashCode();
        return result;
    }

    public void merge(T value) {
        // do nothing
    }

    public void clear() {
        // do nothing
    }
}

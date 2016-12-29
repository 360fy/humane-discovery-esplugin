package io.threesixtyfy.humaneDiscovery.core.tag;

import io.threesixtyfy.humaneDiscovery.core.tokenIndex.TokenIndexConstants;
import io.threesixtyfy.humaneDiscovery.core.utils.GsonUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class TagWithCount<T extends TagWithCount<T>> extends BaseTag<T> {

    private int totalCount;
    private Map<String, List<String>> ancestors;

    public TagWithCount(TagType tagType) {
        super(tagType);
    }

    public TagWithCount(TagType tagType, String name, int totalCount) {
        this(tagType, name, totalCount, null);
    }

    public TagWithCount(TagType tagType, String name, int totalCount, Map<String, List<String>> ancestors) {
        super(tagType, name);
        this.totalCount = totalCount;
        this.ancestors = ancestors;
    }

    public int getTotalCount() {
        return totalCount;
    }

    public Map<String, List<String>> getAncestors() {
        return ancestors;
    }

    @SuppressWarnings("unchecked")
    protected T unmapInternal(Map<String, Object> map) {
        super.unmapInternal(map);

        this.totalCount = (int) map.get(TokenIndexConstants.Fields.TOTAL_COUNT);
        this.ancestors = (Map<String, List<String>>) map.get(TokenIndexConstants.Fields.ANCESTORS);

        return (T) this;
    }

    protected Map<String, Object> map(Map<String, Object> map) {
        super.map(map);

        if (totalCount > 0) {
            map.put(TokenIndexConstants.Fields.TOTAL_COUNT, totalCount);
        }

        if (this.ancestors != null) {
            map.put(TokenIndexConstants.Fields.ANCESTORS, this.ancestors);
        }

        return map;
    }

    public Map<String, Object> map() {
        return this.map(new HashMap<>());
    }

    private List<String> mergeAncestors(List<String> from, List<String> to) {
        if (from == null) {
            return to;
        }

        List<String> finalList = null;
        for (String s : from) {
            boolean found = false;
            for (String t : to) {
                if (StringUtils.equals(s, t)) {
                    found = true;
                    break;
                }
            }

            if (!found) {
                if (finalList == null) {
                    finalList = new ArrayList<>();
                    finalList.addAll(to);
                }

                finalList.add(s);
            }
        }

        if (finalList == null) {
            return to;
        }

        return finalList;
    }

    public void merge(T value) {
        this.totalCount += value.getTotalCount();

        if (value.getAncestors() == null) {
            return;
        }

        if (this.ancestors == null) {
            this.ancestors = value.getAncestors();
        } else {
            for (Map.Entry<String, List<String>> entry : value.getAncestors().entrySet()) {
                String key = entry.getKey();

                if (this.ancestors.containsKey(key)) {
                    List<String> to = this.ancestors.get(key);
                    List<String> from = entry.getValue();

                    this.ancestors.put(key, mergeAncestors(from, to));
                } else {
                    this.ancestors.put(key, entry.getValue());
                }
            }
        }
    }

    public void clear() {
        // do nothing
        if (this.ancestors != null) {
            this.ancestors = null;
        }
    }

    @Override
    public String toString() {
        return GsonUtils.toJson(this);
    }
}

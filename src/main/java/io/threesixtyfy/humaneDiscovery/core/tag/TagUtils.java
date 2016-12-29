package io.threesixtyfy.humaneDiscovery.core.tag;

import io.threesixtyfy.humaneDiscovery.core.tagger.TagWeight;
import io.threesixtyfy.humaneDiscovery.core.tokenIndex.TokenIndexConstants;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class TagUtils {

    public static void unmap(List<Map<String, Object>> tagDataList, Consumer<BaseTag> consumer) {
        unmap(tagDataList, consumer, null, false);
    }

    public static void unmap(List<Map<String, Object>> tagDataList, Consumer<BaseTag> consumer, Map<String, TagWeight> tagWeights, boolean nGram) {
        if (tagDataList == null) {
            return;
        }

        for (Map<String, Object> tagData : tagDataList) {
            TagType tagType = TagType.valueOf((String) tagData.get(TokenIndexConstants.Fields.TAG_TYPE));

            BaseTag tag = null;
            switch (tagType) {
                case Intent:
                    tag = IntentTag.unmap(tagData);
                    break;
                case Keyword:
                    tag = KeywordTag.unmap(tagData);
                    break;
                case StopWord:
                    tag = StopWordTag.unmap(tagData);
                    break;
                case NGram:
                    tag = NGramTag.unmap(tagData);
                    break;
            }

            if (tagWeights != null && tag != null) {
                TagWeight tagWeight = tagWeights.get(tag.getName());
                if (tagWeight != null) {
                    tag.setWeight(tagWeight.getWeight());
                }
            }

            if (nGram && !(tag instanceof NGramTag) && tag != null) {
                tag = new NGramTag(tag.getName(), tag.getTagType(), getTotalCount(tag), getAncestors(tag));
            }

            consumer.accept(tag);
        }
    }

    private static int getTotalCount(BaseTag tag) {
        if (tag instanceof TagWithCount) {
            return ((TagWithCount) tag).getTotalCount();
        }

        return 1;
    }

    private static Map<String, List<String>> getAncestors(BaseTag tag) {
        if (tag instanceof TagWithCount) {
            return ((TagWithCount) tag).getAncestors();
        }

        return null;
    }

}

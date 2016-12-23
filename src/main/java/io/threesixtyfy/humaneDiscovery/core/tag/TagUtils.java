package io.threesixtyfy.humaneDiscovery.core.tag;

import io.threesixtyfy.humaneDiscovery.core.tokenIndex.TokenIndexConstants;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class TagUtils {

    public static void unmap(List<Map<String, Object>> tagDataList, Consumer<BaseTag> consumer) {
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

            consumer.accept(tag);
        }
    }

}

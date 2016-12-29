package io.threesixtyfy.humaneDiscovery.core.instance;

import io.threesixtyfy.humaneDiscovery.core.tagger.TagWeight;
import io.threesixtyfy.humaneDiscovery.core.tokenIndex.TokenIndexConstants;
import org.apache.commons.lang3.StringUtils;

import java.util.Collection;
import java.util.Map;

public interface InstanceContext {

    String getName();

    Collection<TagWeight> getTagWeights();

    Map<String, TagWeight> getTagWeightsMap();

    default String getTagIndex() {
        return StringUtils.lowerCase(getName()) + TokenIndexConstants.TOKEN_STORE_SUFFIX;
    }

}

package io.threesixtyfy.humaneDiscovery.core.tagger;

public interface TagWeight extends Comparable<TagWeight> {

    String getTag();

    float getWeight();

}

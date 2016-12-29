package io.threesixtyfy.humaneDiscovery.core.tagger;

public class DefaultTagWeight implements TagWeight {

    private static final float DEFAULT_WEIGHT = 10.0f;
    private final String tag;

    private final float weight;

    public DefaultTagWeight(String tag) {
        this(tag, DEFAULT_WEIGHT)
        ;
    }

    public DefaultTagWeight(String tag, float weight) {
        this.tag = tag;
        this.weight = weight;
    }

    @Override
    public String getTag() {
        return this.tag;
    }

    @Override
    public float getWeight() {
        return this.weight;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DefaultTagWeight that = (DefaultTagWeight) o;

        return tag.equals(that.tag);
    }

    @Override
    public int hashCode() {
        return tag.hashCode();
    }


    @Override
    public int compareTo(TagWeight o) {
        int ret = Float.compare(o.getWeight(), this.weight);
        if (ret == 0) {
            return this.tag.compareTo(o.getTag());
        }

        return ret;
    }
}

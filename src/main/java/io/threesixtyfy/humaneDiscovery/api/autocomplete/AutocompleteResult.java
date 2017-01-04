package io.threesixtyfy.humaneDiscovery.api.autocomplete;

import io.threesixtyfy.humaneDiscovery.api.commons.BaseResult;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class AutocompleteResult extends BaseResult {

    private static final List<String> NAME_FIELDS = Arrays.asList("brand", "model", "variant");
    private static final List<String> BRAND_FIELDS = Arrays.asList("brand", "brandKey", "brandUrl");
    private static final List<String> MODEL_FIELDS = Arrays.asList("model", "modelKey", "modelUrl");
    private static final List<String> VARIANT_FIELDS = Arrays.asList("variant", "variantKey", "variantUrl");
    private static final String PAGES_FIELD = "pages";
    private static final String NEW_CAR_BRAND_TYPE = "new_car_brand";
    private static final String NEW_CAR_MODEL_TYPE = "new_car_model";
    private static final String NEW_CAR_VARIANT_TYPE = "new_car_variant";
    private static final String NAME_FIELD = "name";
    private static final String KEY_SUFFIX = "Key";
    private static final String URL_SUFFIX = "Url";

    private String id;
    private String type;
    private float score;

    private Map<String, Object> source;

    public AutocompleteResult() {
    }

    public AutocompleteResult(SearchHit searchHit) {
        this(searchHit.id(), searchHit.type(), searchHit.score(), searchHit.sourceRef());
    }

    public AutocompleteResult(String id, String type, float score, BytesReference source) {
        this.id = id;
        this.type = type;
        this.score = score;
        this.source = sourceAsMap(source);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public float getScore() {
        return score;
    }

    public void setScore(float score) {
        this.score = score;
    }

    public Map<String, Object> getSource() {
        return source;
    }

    public void setSource(Map<String, Object> source) {
        this.source = source;
    }

    @SuppressWarnings({"unchecked"})
    public Map<String, Object> sourceAsMap(BytesReference source) throws ElasticsearchParseException {
        if (source == null) {
            return null;
        }


        return SourceLookup.sourceAsMap(source);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        this.id = in.readString();
        this.type = in.readString();
        this.score = in.readFloat();

        this.source = in.readMap();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.id);
        out.writeString(this.type);
        out.writeFloat(this.score);

        // write source
        out.writeMap(this.source);
    }

    @Override
    protected void buildXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(Fields.TYPE, this.type);
        builder.field(Fields._ID, this.id);
        builder.field(Fields._SCORE, this.score);

        this.getSource()
                .entrySet()
                .stream()
                .filter(entry -> {
                    String key = entry.getKey();

                    if (StringUtils.equals(key, PAGES_FIELD)) {
                        return true;
                    }

                    if (StringUtils.equals(this.type, NEW_CAR_BRAND_TYPE)) {
                        return BRAND_FIELDS.contains(key);
                    } else if (StringUtils.equals(this.type, NEW_CAR_MODEL_TYPE)) {
                        return MODEL_FIELDS.contains(key);
                    } else if (StringUtils.equals(this.type, NEW_CAR_VARIANT_TYPE)) {
                        return VARIANT_FIELDS.contains(key);
                    }

                    return false;
                })
                .forEach(entry -> {
                    try {
                        String key = entry.getKey();

                        if (StringUtils.endsWith(key, KEY_SUFFIX)) {
                            key = "key";
                        } else if (StringUtils.endsWith(key, URL_SUFFIX)) {
                            key = "url";
                        } else if (NAME_FIELDS.contains(key)) {
                            key = NAME_FIELD;
                        }

                        if (StringUtils.equals(this.type, NEW_CAR_BRAND_TYPE) && StringUtils.equals(key, NAME_FIELD)) {
                            builder.field("key", entry.getValue());
                        }

                        builder.field(key, entry.getValue());
                    } catch (IOException e) {
                        // ignore error
                    }
                });
    }

    private static final class Fields {
        static final String TYPE = "type";
        static final String _ID = "_id";
        static final String _SCORE = "_score";
    }
}

package io.threesixtyfy.humaneDiscovery.api.commons;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public abstract class BaseResult implements Streamable, ToXContent {

    public BaseResult() {
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        buildXContent(builder, params);

        builder.endObject();
        return builder;
    }

    protected abstract void buildXContent(XContentBuilder builder, Params params) throws IOException;

    @JsonIgnore
    public long getCount() {
        return 1;
    }
}

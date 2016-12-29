package io.threesixtyfy.humaneDiscovery.api.commons;

import io.threesixtyfy.humaneDiscovery.core.utils.XContentUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.FromXContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

public abstract class QuerySource<T extends QuerySource> implements FromXContentBuilder<QuerySource> {

    protected static final ParseField QUERY_FIELD = new ParseField("query").withDeprecation("text", "q");

    protected String query;

    public abstract String key();

    public ActionRequestValidationException validate(ActionRequestValidationException validationException) {
        if (StringUtils.isBlank(query)) {
            validationException = ValidateActions.addValidationError("query is missing", validationException);
        }

        return validationException;
    }

    public String query() {
        return this.query;
    }

    public QuerySource query(String query) {
        this.query = StringUtils.lowerCase(StringUtils.trim(query));
        return this;
    }

    public void readFrom(StreamInput in) throws IOException {
        query = in.readString();
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(query);
    }

    @Override
    public QuerySource<T> fromXContent(XContentParser parser, ParseFieldMatcher parseFieldMatcher) throws IOException {
        XContentUtils.parseObject(parser, parseFieldMatcher, this::parseFields);

        return this;
    }

    protected void parseFields(XContentParser parser, ParseFieldMatcher parseFieldMatcher) {
        XContentParser.Token token = parser.currentToken();
        String fieldName = XContentUtils.currentName(parser);

        if (parseFieldMatcher.match(fieldName, QUERY_FIELD)) {
            parseQueryField(parser);
        } else {
            throw new ElasticsearchParseException("failed to parseObject. expected 'query' but got [{}]", token);
        }
    }

    protected void parseQueryField(XContentParser parser) {
        XContentParser.Token token = XContentUtils.nextToken(parser);

        if (token.isValue()) {
            this.query(XContentUtils.text(parser));
        } else {
            throw new ElasticsearchParseException("failed to parseObject. expected value but got [{}]", token);
        }
    }

    @Override
    public String toString() {
        return "{" +
                "query='" + query + '\'' +
                '}';
    }
}

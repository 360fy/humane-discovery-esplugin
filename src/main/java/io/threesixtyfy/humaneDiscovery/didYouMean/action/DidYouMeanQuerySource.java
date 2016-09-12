package io.threesixtyfy.humaneDiscovery.didYouMean.action;

import io.threesixtyfy.humaneDiscovery.commons.action.BaseQuerySource;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

public class DidYouMeanQuerySource extends BaseQuerySource<DidYouMeanQuerySource> {

    @Override
    public DidYouMeanQuerySource fromXContent(XContentParser parser, ParseFieldMatcher parseFieldMatcher) throws IOException {
        XContentParser.Token token;
        token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new ElasticsearchParseException("failed to parse. source must be an object, but found [{}] instead", token.name());
        }

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                String fieldName = parser.currentName();

                if (!"query".equals(fieldName)) {
                    throw new ElasticsearchParseException("failed to parse. expected 'query' but got [{}]", token);
                }

                token = parser.nextToken();

                if (token.isValue()) {
                    this.query(parser.text());
                } else {
                    throw new ElasticsearchParseException("failed to parse. expected value but got [{}]", token);
                }
            } else {
                if (token == null) {
                    throw new ElasticsearchParseException("failed to parse search source. end of query source reached but query is not complete.");
                } else {
                    throw new ElasticsearchParseException("failed to parse search source. expected field name but got [{}]", token);
                }
            }
        }

        return this;
    }

}

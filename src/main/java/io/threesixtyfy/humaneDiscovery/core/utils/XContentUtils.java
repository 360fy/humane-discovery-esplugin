package io.threesixtyfy.humaneDiscovery.core.utils;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.function.BiConsumer;

public class XContentUtils {

    public static void parseObject(XContentParser parser, ParseFieldMatcher parseFieldMatcher, BiConsumer<XContentParser, ParseFieldMatcher> fieldParser) throws IOException {
        XContentParser.Token token;
        token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new ElasticsearchParseException("failed to parseObject. source must be an object, but found [{}] instead", token.name());
        }

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldParser.accept(parser, parseFieldMatcher);
            } else {
                if (token == null) {
                    throw new ElasticsearchParseException("failed to parseObject search source. end of query source reached but query is not complete.");
                } else {
                    throw new ElasticsearchParseException("failed to parseObject search source. expected field name but got [{}]", token);
                }
            }
        }
    }

    public static XContentParser.Token nextToken(XContentParser parser) {
        try {
            return parser.nextToken();
        } catch (IOException e) {
            throw new ElasticsearchParseException("IOException in retrieving next token from parser [{}]", parser);
        }
    }

    public static String text(XContentParser parser) {
        try {
            return parser.text();
        } catch (IOException e) {
            throw new ElasticsearchParseException("IOException in retrieving text from parser [{}]", parser);
        }
    }

    public static int integer(XContentParser parser) {
        try {
            return parser.intValue();
        } catch (IOException e) {
            throw new ElasticsearchParseException("IOException in retrieving text from parser [{}]", parser);
        }
    }

    public static String currentName(XContentParser parser) {
        try {
            return parser.currentName();
        } catch (IOException e) {
            throw new ElasticsearchParseException("IOException in retrieving currentName from parser [{}]", parser);
        }
    }

}

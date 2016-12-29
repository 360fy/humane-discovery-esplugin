package io.threesixtyfy.humaneDiscovery.api.autocomplete;

import io.threesixtyfy.humaneDiscovery.api.commons.QuerySource;
import io.threesixtyfy.humaneDiscovery.core.utils.XContentUtils;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.threesixtyfy.humaneDiscovery.api.search.SearchConstants.COUNT_FIELD;
import static io.threesixtyfy.humaneDiscovery.api.search.SearchConstants.DEFAULT_COUNT;
import static io.threesixtyfy.humaneDiscovery.api.search.SearchConstants.FILTER_FIELD;
import static io.threesixtyfy.humaneDiscovery.api.search.SearchConstants.FORMAT_FIELD;
import static io.threesixtyfy.humaneDiscovery.api.search.SearchConstants.PAGE_FIELD;
import static io.threesixtyfy.humaneDiscovery.api.search.SearchConstants.SECTION_FIELD;
import static io.threesixtyfy.humaneDiscovery.api.search.SearchConstants.SORT_FIELD;
import static io.threesixtyfy.humaneDiscovery.api.search.SearchConstants.TYPE_FIELD;

public class AutocompleteQuerySource extends QuerySource<AutocompleteQuerySource> {

    private int count = DEFAULT_COUNT;
    private int page;
    private String type;
    private String section;
    private String format;
    private String key;

    public int count() {
        return count;
    }

    public AutocompleteQuerySource count(int count) {
        this.count = count;
        return this;
    }

    public int page() {
        return page;
    }

    public AutocompleteQuerySource page(int page) {
        this.page = page;
        return this;
    }

    public String type() {
        return type;
    }

    public AutocompleteQuerySource type(String type) {
        this.type = type;
        return this;
    }

    public String section() {
        return section;
    }

    public AutocompleteQuerySource section(String section) {
        this.section = section;
        return this;
    }

    public String format() {
        return format;
    }

    public AutocompleteQuerySource format(String format) {
        this.format = format;
        return this;
    }

    @Override
    protected void parseFields(XContentParser parser, ParseFieldMatcher parseFieldMatcher) {
        XContentParser.Token token = parser.currentToken();
        String fieldName = XContentUtils.currentName(parser);

        if (parseFieldMatcher.match(fieldName, QUERY_FIELD)) {
            parseQueryField(parser);
        } else if (parseFieldMatcher.match(fieldName, COUNT_FIELD)) {
            parseCountField(parser);
        } else if (parseFieldMatcher.match(fieldName, PAGE_FIELD)) {
            parsePageField(parser);
        } else if (parseFieldMatcher.match(fieldName, TYPE_FIELD)) {
            parseTypeField(parser);
        } else if (parseFieldMatcher.match(fieldName, SECTION_FIELD)) {
            parseSectionField(parser);
        } else if (parseFieldMatcher.match(fieldName, SORT_FIELD)) {
            parseSortField(parser);
        } else if (parseFieldMatcher.match(fieldName, FILTER_FIELD)) {
            parseFilterField(parser);
        } else if (parseFieldMatcher.match(fieldName, FORMAT_FIELD)) {
            parseFormatField(parser);
        } else {
            throw new ElasticsearchParseException("failed to parseObject. expected 'query' but got [{}]", token);
        }
    }

    private void parseCountField(XContentParser parser) {
        XContentParser.Token token = XContentUtils.nextToken(parser);

        if (token.isValue()) {
            this.count(XContentUtils.integer(parser));
        } else {
            throw new ElasticsearchParseException("failed to parseObject. expected value but got [{}]", token);
        }
    }

    private void parsePageField(XContentParser parser) {
        XContentParser.Token token = XContentUtils.nextToken(parser);

        if (token.isValue()) {
            this.page(XContentUtils.integer(parser));
        } else {
            throw new ElasticsearchParseException("failed to parseObject. expected value but got [{}]", token);
        }
    }

    private void parseTypeField(XContentParser parser) {
        XContentParser.Token token = XContentUtils.nextToken(parser);

        if (token.isValue()) {
            this.type(XContentUtils.text(parser));
        } else {
            throw new ElasticsearchParseException("failed to parseObject. expected value but got [{}]", token);
        }
    }

    private void parseSectionField(XContentParser parser) {
        XContentParser.Token token = XContentUtils.nextToken(parser);

        if (token.isValue()) {
            this.section(XContentUtils.text(parser));
        } else {
            throw new ElasticsearchParseException("failed to parseObject. expected value but got [{}]", token);
        }
    }

    private void parseSortField(XContentParser parser) {
//        XContentParser.Token token = XContentUtils.nextToken(parser);
//
//        if (token.isValue()) {
//            this.query(XContentUtils.text(parser));
//        } else {
//            throw new ElasticsearchParseException("failed to parseObject. expected value but got [{}]", token);
//        }
    }

    private void parseFilterField(XContentParser parser) {
//        XContentParser.Token token = XContentUtils.nextToken(parser);
//
//        if (token.isValue()) {
//            this.query(XContentUtils.text(parser));
//        } else {
//            throw new ElasticsearchParseException("failed to parseObject. expected value but got [{}]", token);
//        }
    }

    private void parseFormatField(XContentParser parser) {
        XContentParser.Token token = XContentUtils.nextToken(parser);

        if (token.isValue()) {
            this.format(XContentUtils.text(parser));
        } else {
            throw new ElasticsearchParseException("failed to parseObject. expected value but got [{}]", token);
        }
    }

    @Override
    public String key() {
        if (key == null) {
            key = Stream.of(this.type(), this.section(), this.query(), this.format(), String.valueOf(this.page()), String.valueOf(this.count()))
                    .filter(Objects::nonNull)
                    .collect(Collectors.joining(":"));
        }

        return key;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);

        this.count = in.readInt();
        this.page = in.readOptionalVInt();
        this.type = in.readOptionalString();
        this.section = in.readOptionalString();
        this.format = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        out.writeInt(this.count);
        out.writeOptionalVInt(this.page);
        out.writeOptionalString(this.type);
        out.writeOptionalString(this.section);
        out.writeOptionalString(this.format);
    }
}

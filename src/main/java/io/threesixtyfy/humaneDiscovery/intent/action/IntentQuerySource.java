package io.threesixtyfy.humaneDiscovery.intent.action;

import io.threesixtyfy.humaneDiscovery.commons.action.BaseQuerySource;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class IntentQuerySource extends BaseQuerySource<IntentQuerySource> {

    public static final String TAG_QUERY = "query";
    public static final String TAG_FIELDS = "fields";
    public static final String TAG_REGEX_ENTITIES = "regexEntities";
    public static final String TAG_NAME = "name";
    public static final String TAG_REGEX = "regex";
    public static final String TAG_RESULT = "result";

    private String[] fields = Strings.EMPTY_ARRAY;

    private IntentEntity[] intentEntities;

    public String[] fields() {
        return fields;
    }

    public IntentQuerySource fields(String... fields) {
        this.fields = fields;
        return this;
    }

    public IntentEntity[] intentEntities() {
        return intentEntities;
    }

    public IntentQuerySource intentEntities(IntentEntity... entities) {
        this.intentEntities = entities;
        return this;
    }

    @Override
    public IntentQuerySource fromXContent(XContentParser parser, ParseFieldMatcher parseFieldMatcher) throws IOException {
        XContentParser.Token token;
        token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new ElasticsearchParseException("failed to parse. source must be an object, but found [{}] instead", token.name());
        }

        String currentFieldName = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                String fieldName = parser.currentName();

                if (TAG_QUERY.equals(fieldName) || TAG_FIELDS.equals(fieldName) || TAG_REGEX_ENTITIES.equals(fieldName)) {
                    currentFieldName = fieldName;
                } else {
                    throw new ElasticsearchParseException("failed to parse. got unknown field name [{}] for token [{}]", fieldName, token);
                }
            } else if (TAG_QUERY.equals(currentFieldName)) {
                parseQuery(parser, token);
            } else if (TAG_FIELDS.equals(currentFieldName)) {
                parseFields(parser, token);
            } else if (TAG_REGEX_ENTITIES.equals(currentFieldName)) {
                parseIntentEntities(parser, token);
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

    private void parseIntentEntities(XContentParser parser, XContentParser.Token token) throws IOException {
        if (token == XContentParser.Token.START_ARRAY) {
            // parse fields
            List<IntentEntity> intentEntities = new ArrayList<>();

            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                if (token == XContentParser.Token.START_OBJECT) {
                    intentEntities.add(parseRegexIntentObject(parser));
                } else {
                    throw new ElasticsearchParseException("expected intent entity object but got [{}]", token);
                }
            }

            this.intentEntities(intentEntities.toArray(new IntentEntity[intentEntities.size()]));
        } else {
            throw new ElasticsearchParseException("expected array of intent entities but got [{}]", token);
        }
    }

    private RegexIntentEntity parseRegexIntentObject(XContentParser parser) throws IOException {
        // we have object
        RegexIntentEntity regexIntentEntity = new RegexIntentEntity();

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (TAG_NAME.equals(currentFieldName)) {
                regexIntentEntity.setName(parseSimpleValue(parser, token));
            } else if (TAG_REGEX.equals(currentFieldName)) {
                regexIntentEntity.setRegex(parseSimpleValue(parser, token));
            } else if (TAG_RESULT.equals(currentFieldName)) {
                parseRegexIntentResultFields(parser, token, regexIntentEntity);
            } else {
                throw new ElasticsearchParseException("got unexpected field [{}] for token [{}]", currentFieldName, token);
            }
        }

        return regexIntentEntity;
    }

    private void parseRegexIntentResultFields(XContentParser parser, XContentParser.Token token, RegexIntentEntity regexIntentEntity) throws IOException {
        if (token == XContentParser.Token.START_OBJECT) {
            // parse fields
            List<IntentEntity.ResultField> resultFields = new ArrayList<>();

            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    String value = parser.text();
                    IntentEntity.ResultField resultField;
                    try {
                        int number = Integer.parseInt(value);
                        // MatchResultField
                        IntentEntity.MatchResultField matchResultField = new IntentEntity.MatchResultField();
                        matchResultField.setMatchIndex(number);

                        resultField = matchResultField;
                    } catch (NumberFormatException nfe) {
                        // FixedResultField
                        IntentEntity.FixedResultField fixedResultField = new IntentEntity.FixedResultField();
                        fixedResultField.setValue(value);

                        resultField = fixedResultField;
                    }

                    resultField.setKey(currentFieldName);
                    resultFields.add(resultField);
                } else {
                    throw new ElasticsearchParseException("failed to parse. expected simple value but got [{}]", token);
                }
            }

            regexIntentEntity.setResultFields(resultFields.toArray(new IntentEntity.ResultField[resultFields.size()]));
        } else {
            throw new ElasticsearchParseException("expected array of fields but got [{}]", token);
        }
    }

    private void parseFields(XContentParser parser, XContentParser.Token token) throws IOException {
        if (token == XContentParser.Token.START_ARRAY) {
            // parse fields
            List<String> fields = new ArrayList<>();

            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                // we have simple field
                fields.add(parseSimpleValue(parser, token));
            }

            this.fields(fields.toArray(new String[fields.size()]));
        } else {
            throw new ElasticsearchParseException("expected array of fields but got [{}]", token);
        }
    }

    private void parseQuery(XContentParser parser, XContentParser.Token token) throws IOException {
        this.query(parseSimpleValue(parser, token));
    }

    private String parseSimpleValue(XContentParser parser, XContentParser.Token token) throws IOException {
        if (token.isValue()) {
            return parser.text();
        } else {
            throw new ElasticsearchParseException("failed to parse. expected simple value but got [{}]", token);
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);

        this.fields = in.readStringArray();

        int numIntentEntities = in.readVInt();
        this.intentEntities = new IntentEntity[numIntentEntities];

        for (int i = 0; i < numIntentEntities; i++) {
            // read intent entities
            this.intentEntities[i] = new RegexIntentEntity();
            this.intentEntities[i].readFrom(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        out.writeStringArray(fields);

        out.writeVInt(intentEntities.length);
        for (IntentEntity intentEntity : intentEntities) {
            intentEntity.writeTo(out);
        }
    }

    @Override
    public String toString() {
        return "{" +
                "fields=" + Arrays.toString(fields) +
                ", intentEntities=" + Arrays.toString(intentEntities) +
                "} " + super.toString();
    }
}

package io.threesixtyfy.humaneDiscovery.api.intent;

import io.threesixtyfy.humaneDiscovery.api.commons.BaseQuerySource;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class IntentQuerySource extends BaseQuerySource<IntentQuerySource> {

    public static final String TAG_QUERY = "query";
    public static final String TAG_LOOKUP_ENTITIES = "lookupEntities";
    public static final String TAG_REGEX_ENTITIES = "regexEntities";
    public static final String TAG_NAME = "name";
    public static final String TAG_CLASS = "entityClass";
    public static final String TAG_WEIGHT = "weight";
    public static final String TAG_REGEX = "regex";
    public static final String TAG_RESULT = "result";

    private Set<LookupIntentEntity> lookupEntities;

    private Set<RegexIntentEntity> regexEntities;

    public Set<LookupIntentEntity> getLookupEntities() {
        return lookupEntities;
    }

    public void setLookupEntities(Set<LookupIntentEntity> lookupEntities) {
        this.lookupEntities = lookupEntities;
    }

    public Set<RegexIntentEntity> getRegexEntities() {
        return regexEntities;
    }

    public void setRegexEntities(Set<RegexIntentEntity> regexEntities) {
        this.regexEntities = regexEntities;
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

                if (TAG_QUERY.equals(fieldName) || TAG_LOOKUP_ENTITIES.equals(fieldName) || TAG_REGEX_ENTITIES.equals(fieldName)) {
                    currentFieldName = fieldName;
                } else {
                    throw new ElasticsearchParseException("failed to parse. got unknown field name [{}] for token [{}]", fieldName, token);
                }
            } else if (TAG_QUERY.equals(currentFieldName)) {
                parseQuery(parser, token);
            } else if (TAG_LOOKUP_ENTITIES.equals(currentFieldName)) {
                parseLookupEntities(parser, token);
            } else if (TAG_REGEX_ENTITIES.equals(currentFieldName)) {
                parseRegexEntities(parser, token);
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

    private void parseRegexEntities(XContentParser parser, XContentParser.Token token) throws IOException {
        if (token == XContentParser.Token.START_ARRAY) {
            // parse intentFields
            this.regexEntities = new HashSet<>();

            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                if (token == XContentParser.Token.START_OBJECT) {
                    this.regexEntities.add(parseRegexIntentObject(parser));
                } else {
                    throw new ElasticsearchParseException("expected intent entity object but got [{}]", token);
                }
            }
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
                regexIntentEntity.setEntityName(parseStringValue(parser, token));
            } else if (TAG_WEIGHT.equals(currentFieldName)) {
                regexIntentEntity.setWeight(parseFloatValue(parser, token));
            } else if (TAG_REGEX.equals(currentFieldName)) {
                regexIntentEntity.setRegex(parseStringValue(parser, token));
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
            // parse intentFields
            regexIntentEntity.setResultFields(new HashSet<>());

            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    String value = parser.text();
                    RegexIntentEntity.ResultField resultField;
                    try {
                        int number = Integer.parseInt(value);
                        // MatchResultField
                        RegexIntentEntity.MatchResultField matchResultField = new RegexIntentEntity.MatchResultField();
                        matchResultField.setMatchIndex(number);

                        resultField = matchResultField;
                    } catch (NumberFormatException nfe) {
                        // FixedResultField
                        RegexIntentEntity.FixedResultField fixedResultField = new RegexIntentEntity.FixedResultField();
                        fixedResultField.setValue(value);

                        resultField = fixedResultField;
                    }

                    resultField.setKey(currentFieldName);
                    regexIntentEntity.getResultFields().add(resultField);
                } else {
                    throw new ElasticsearchParseException("failed to parse. expected simple value but got [{}]", token);
                }
            }
        } else {
            throw new ElasticsearchParseException("expected array of intentFields but got [{}]", token);
        }
    }

    private void parseLookupEntities(XContentParser parser, XContentParser.Token token) throws IOException {
        if (token == XContentParser.Token.START_ARRAY) {
            this.lookupEntities = new HashSet<>();
            // parse intentFields
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                // we have simple field
                if (token == XContentParser.Token.START_OBJECT) {
                    this.lookupEntities.add(parseLookupIntentObject(parser));
                } else {
                    throw new ElasticsearchParseException("expected intent entity object but got [{}]", token);
                }
            }
        } else {
            throw new ElasticsearchParseException("expected array of intentFields but got [{}]", token);
        }
    }

    private LookupIntentEntity parseLookupIntentObject(XContentParser parser) throws IOException {
        // we have object
        LookupIntentEntity lookupIntentEntity = new LookupIntentEntity();

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (TAG_NAME.equals(currentFieldName)) {
                lookupIntentEntity.setEntityName(StringUtils.lowerCase(parseStringValue(parser, token)));
            } else if (TAG_CLASS.equals(currentFieldName)) {
                lookupIntentEntity.setEntityClass(StringUtils.lowerCase(parseStringValue(parser, token)));
            } else if (TAG_WEIGHT.equals(currentFieldName)) {
                lookupIntentEntity.setWeight(parseFloatValue(parser, token));
            } else {
                throw new ElasticsearchParseException("got unexpected field [{}] for token [{}]", currentFieldName, token);
            }
        }

        return lookupIntentEntity;
    }

    private void parseQuery(XContentParser parser, XContentParser.Token token) throws IOException {
        this.query(parseStringValue(parser, token));
    }

    private String parseStringValue(XContentParser parser, XContentParser.Token token) throws IOException {
        if (token.isValue()) {
            return parser.text();
        } else {
            throw new ElasticsearchParseException("failed to parse. expected simple value but got [{}]", token);
        }
    }

    private float parseFloatValue(XContentParser parser, XContentParser.Token token) throws IOException {
        if (token.isValue()) {
            return parser.floatValue();
        } else {
            throw new ElasticsearchParseException("failed to parse. expected simple value but got [{}]", token);
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);

        int numLookupEntities = in.readVInt();
        this.lookupEntities = new HashSet<>(numLookupEntities);
        for (int i = 0; i < numLookupEntities; i++) {
            // read intent entities
            LookupIntentEntity lookupIntentEntity = new LookupIntentEntity();
            lookupIntentEntity.readFrom(in);
            this.lookupEntities.add(lookupIntentEntity);
        }

        int numRegexEntities = in.readVInt();
        this.regexEntities = new HashSet<>(numRegexEntities);

        for (int i = 0; i < numRegexEntities; i++) {
            // read intent entities
            RegexIntentEntity regexIntentEntity = new RegexIntentEntity();
            regexIntentEntity.readFrom(in);
            this.regexEntities.add(regexIntentEntity);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        out.writeVInt(lookupEntities.size());
        for (LookupIntentEntity lookupIntentEntity : lookupEntities) {
            lookupIntentEntity.writeTo(out);
        }

        out.writeVInt(regexEntities.size());
        for (IntentEntity intentEntity : regexEntities) {
            intentEntity.writeTo(out);
        }
    }

    @Override
    public String toString() {
        return "{" +
                "lookupEntities=" + lookupEntities +
                ", regexEntities=" + regexEntities +
                "} " + super.toString();
    }
}

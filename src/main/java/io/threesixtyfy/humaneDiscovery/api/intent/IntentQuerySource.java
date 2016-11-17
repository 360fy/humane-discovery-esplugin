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

    private static final String QUERY_TAG = "query";
    private static final String LOOKUP_ENTITIES_TAG = "lookupEntities";
    private static final String REGEX_ENTITIES_TAG = "regexEntities";
    private static final String NAME_TAG = "name";
    private static final String CLASS_TAG = "entityClass";
    private static final String WEIGHT_TAG = "weight";
    private static final String REGEX_TAG = "regex";
    private static final String RESULT_TAG = "result";

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

                if (QUERY_TAG.equals(fieldName) || LOOKUP_ENTITIES_TAG.equals(fieldName) || REGEX_ENTITIES_TAG.equals(fieldName)) {
                    currentFieldName = fieldName;
                } else {
                    throw new ElasticsearchParseException("failed to parse. got unknown field name [{}] for token [{}]", fieldName, token);
                }
            } else if (QUERY_TAG.equals(currentFieldName)) {
                parseQuery(parser, token);
            } else if (LOOKUP_ENTITIES_TAG.equals(currentFieldName)) {
                parseLookupEntities(parser, token);
            } else if (REGEX_ENTITIES_TAG.equals(currentFieldName)) {
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
            } else if (NAME_TAG.equals(currentFieldName)) {
                regexIntentEntity.setEntityName(parseStringValue(parser, token));
            } else if (WEIGHT_TAG.equals(currentFieldName)) {
                regexIntentEntity.setWeight(parseFloatValue(parser, token));
            } else if (REGEX_TAG.equals(currentFieldName)) {
                regexIntentEntity.setRegex(parseStringValue(parser, token));
            } else if (RESULT_TAG.equals(currentFieldName)) {
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
            } else if (NAME_TAG.equals(currentFieldName)) {
                lookupIntentEntity.setEntityName(StringUtils.lowerCase(parseStringValue(parser, token)));
            } else if (CLASS_TAG.equals(currentFieldName)) {
                lookupIntentEntity.setEntityClass(StringUtils.lowerCase(parseStringValue(parser, token)));
            } else if (WEIGHT_TAG.equals(currentFieldName)) {
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

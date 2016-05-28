package io.threesixtyfy.humaneDiscovery.mapper;

import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.core.StringFieldMapper;

import java.util.Iterator;
import java.util.Map;

import static org.elasticsearch.index.mapper.core.TypeParsers.parseMultiField;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseTextField;

public class HumaneTextFieldMapper extends StringFieldMapper {

    public static final String CONTENT_TYPE = "humane_text";

    private static final int POSITION_INCREMENT_GAP_USE_ANALYZER = -1;

    public static class Defaults {
        public static final MappedFieldType FIELD_TYPE = new HumaneTextFieldType();

        static {
            FIELD_TYPE.freeze();
        }

        public static final int IGNORE_ABOVE = -1;
    }

    public static class Builder extends FieldMapper.Builder<HumaneTextFieldMapper.Builder, HumaneTextFieldMapper> {

        /**
         * The distance between tokens from different values in the same field.
         * POSITION_INCREMENT_GAP_USE_ANALYZER means default to the analyzer's
         * setting which in turn defaults to Defaults.POSITION_INCREMENT_GAP.
         */
        protected int positionIncrementGap = POSITION_INCREMENT_GAP_USE_ANALYZER;

        protected int ignoreAbove = HumaneTextFieldMapper.Defaults.IGNORE_ABOVE;

        public Builder(String name) {
            this(name, HumaneTextFieldMapper.Defaults.FIELD_TYPE, HumaneTextFieldMapper.Defaults.FIELD_TYPE);
        }

        protected Builder(String name, MappedFieldType fieldType, MappedFieldType defaultFieldType) {
            super(name, fieldType, defaultFieldType);
            builder = this;
        }

        @Override
        public Builder searchAnalyzer(NamedAnalyzer searchAnalyzer) {
            super.searchAnalyzer(searchAnalyzer);
            return this;
        }

        public Builder positionIncrementGap(int positionIncrementGap) {
            this.positionIncrementGap = positionIncrementGap;
            return this;
        }

        public Builder searchQuotedAnalyzer(NamedAnalyzer analyzer) {
            this.fieldType.setSearchQuoteAnalyzer(analyzer);
            return builder;
        }

        public Builder suggestionWeight(float weight) {
            ((HumaneTextFieldType) this.fieldType).setSuggestionWeight(weight);
            return builder;
        }

        public Builder ignoreAbove(int ignoreAbove) {
            this.ignoreAbove = ignoreAbove;
            return this;
        }

        protected HumaneTextFieldMapper buildMapper(Mapper.BuilderContext context) {
            HumaneTextFieldMapper fieldMapper = new HumaneTextFieldMapper(
                    name, fieldType, defaultFieldType, positionIncrementGap, ignoreAbove,
                    context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);

            return (HumaneTextFieldMapper) fieldMapper.includeInAll(includeInAll);
        }

        @Override
        public HumaneTextFieldMapper build(Mapper.BuilderContext context) {
            if (positionIncrementGap != POSITION_INCREMENT_GAP_USE_ANALYZER) {
                fieldType.setIndexAnalyzer(new NamedAnalyzer(fieldType.indexAnalyzer(), positionIncrementGap));
                fieldType.setSearchAnalyzer(new NamedAnalyzer(fieldType.searchAnalyzer(), positionIncrementGap));
                fieldType.setSearchQuoteAnalyzer(new NamedAnalyzer(fieldType.searchQuoteAnalyzer(), positionIncrementGap));
            }
            // if the field is not analyzed, then by default, we should omit norms and have docs only
            // index options, as probably what the user really wants
            // if they are set explicitly, we will use those values
            // we also change the values on the default field type so that toXContent emits what
            // differs from the defaults
            if (fieldType.indexOptions() != IndexOptions.NONE && !fieldType.tokenized()) {
                defaultFieldType.setOmitNorms(true);
                defaultFieldType.setIndexOptions(IndexOptions.DOCS);
                if (!omitNormsSet && fieldType.boost() == 1.0f) {
                    fieldType.setOmitNorms(true);
                }
                if (!indexOptionsSet) {
                    fieldType.setIndexOptions(IndexOptions.DOCS);
                }
            }

            setupFieldType(context);

            return buildMapper(context);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {

        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            return parse(new Builder(name), name, node, parserContext);
        }

        public Mapper.Builder parse(HumaneTextFieldMapper.Builder builder, String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            parseTextField(builder, name, node, parserContext);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext(); ) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = Strings.toUnderscoreCase(entry.getKey());
                Object propNode = entry.getValue();
                if (propName.equals("null_value")) {
                    if (propNode == null) {
                        throw new MapperParsingException("Property [null_value] cannot be null.");
                    }
                    builder.nullValue(propNode.toString());
                    iterator.remove();
                } else if (propName.equals("suggestion_weight")) {
                    if (propNode == null) {
                        throw new MapperParsingException("Property [suggestion_weight] cannot be null.");
                    }
                    builder.suggestionWeight(Float.parseFloat(propNode.toString()));
                    iterator.remove();
                } else if (propName.equals("search_quote_analyzer")) {
                    NamedAnalyzer analyzer = parserContext.analysisService().analyzer(propNode.toString());
                    if (analyzer == null) {
                        throw new MapperParsingException("Analyzer [" + propNode.toString() + "] not found for field [" + name + "]");
                    }
                    builder.searchQuotedAnalyzer(analyzer);
                    iterator.remove();
                } else if (propName.equals("position_increment_gap") ||
                        parserContext.indexVersionCreated().before(Version.V_2_0_0) && propName.equals("position_offset_gap")) {
                    int newPositionIncrementGap = XContentMapValues.nodeIntegerValue(propNode, -1);
                    if (newPositionIncrementGap < 0) {
                        throw new MapperParsingException("positions_increment_gap less than 0 aren't allowed.");
                    }
                    builder.positionIncrementGap(newPositionIncrementGap);
                    // we need to update to actual analyzers if they are not set in this case...
                    // so we can inject the position increment gap...
                    if (builder.fieldType().indexAnalyzer() == null) {
                        builder.fieldType().setIndexAnalyzer(parserContext.analysisService().defaultIndexAnalyzer());
                    }
                    if (builder.fieldType().searchAnalyzer() == null) {
                        builder.fieldType().setSearchAnalyzer(parserContext.analysisService().defaultSearchAnalyzer());
                    }
                    if (builder.fieldType().searchQuoteAnalyzer() == null) {
                        builder.fieldType().setSearchQuoteAnalyzer(parserContext.analysisService().defaultSearchQuoteAnalyzer());
                    }
                    iterator.remove();
                } else if (propName.equals("ignore_above")) {
                    builder.ignoreAbove(XContentMapValues.nodeIntegerValue(propNode, -1));
                    iterator.remove();
                } else if (parseMultiField(builder, name, parserContext, propName, propNode)) {
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    public static class HumaneTextFieldType extends MappedFieldType {

        protected float suggestionWeight = 1.0f;

        public HumaneTextFieldType() {
        }

        protected HumaneTextFieldType(HumaneTextFieldType ref) {
            super(ref);
        }

        public HumaneTextFieldType clone() {
            return new HumaneTextFieldType(this);
        }

        public float getSuggestionWeight() {
            return suggestionWeight;
        }

        public void setSuggestionWeight(float suggestionWeight) {
            this.suggestionWeight = suggestionWeight;
        }

        @Override
        public String typeName() {
            return "string";
        }

        @Override
        public String value(Object value) {
            if (value == null) {
                return null;
            }
            return value.toString();
        }

        @Override
        public Query nullValueQuery() {
            if (nullValue() == null) {
                return null;
            }
            return termQuery(nullValue(), null);
        }
    }

    protected HumaneTextFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                                    int positionIncrementGap, int ignoreAbove,
                                    Settings indexSettings, FieldMapper.MultiFields multiFields, FieldMapper.CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, positionIncrementGap, ignoreAbove, indexSettings, multiFields, copyTo);
    }

    @Override
    protected HumaneTextFieldMapper clone() {
        return (HumaneTextFieldMapper) super.clone();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}

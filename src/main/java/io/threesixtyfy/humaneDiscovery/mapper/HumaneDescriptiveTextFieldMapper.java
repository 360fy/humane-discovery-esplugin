package io.threesixtyfy.humaneDiscovery.mapper;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;

import java.util.Map;

public class HumaneDescriptiveTextFieldMapper extends HumaneTextFieldMapper {

    public static final String CONTENT_TYPE = "humane_descriptive_text";

    public static class Defaults {
        public static final MappedFieldType FIELD_TYPE = new HumaneDescriptiveTextFieldType();

        static {
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends HumaneTextFieldMapper.Builder {

        public Builder(String name) {
            super(name, HumaneDescriptiveTextFieldMapper.Defaults.FIELD_TYPE, HumaneDescriptiveTextFieldMapper.Defaults.FIELD_TYPE);
        }

        public Builder suggestionWeight(float weight) {
            ((HumaneTextFieldType) this.fieldType).setSuggestionWeight(weight);
            return this;
        }

        protected HumaneDescriptiveTextFieldMapper buildMapper(Mapper.BuilderContext context) {
            HumaneDescriptiveTextFieldMapper fieldMapper = new HumaneDescriptiveTextFieldMapper(
                    name, fieldType, defaultFieldType, positionIncrementGap, ignoreAbove,
                    context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);

            return (HumaneDescriptiveTextFieldMapper) fieldMapper.includeInAll(includeInAll);
        }
    }

    public static class TypeParser extends HumaneTextFieldMapper.TypeParser {

        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            return parse(new Builder(name), name, node, parserContext);
        }

    }

    public static class HumaneDescriptiveTextFieldType extends MappedFieldType {

        protected float suggestionWeight = 0.2f;

        public HumaneDescriptiveTextFieldType() {
        }

        protected HumaneDescriptiveTextFieldType(HumaneDescriptiveTextFieldType ref) {
            super(ref);
        }

        public HumaneDescriptiveTextFieldType clone() {
            return new HumaneDescriptiveTextFieldType(this);
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

    protected HumaneDescriptiveTextFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                                               int positionIncrementGap, int ignoreAbove,
                                               Settings indexSettings, FieldMapper.MultiFields multiFields, FieldMapper.CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, positionIncrementGap, ignoreAbove, indexSettings, multiFields, copyTo);
    }

    @Override
    protected HumaneDescriptiveTextFieldMapper clone() {
        return (HumaneDescriptiveTextFieldMapper) super.clone();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
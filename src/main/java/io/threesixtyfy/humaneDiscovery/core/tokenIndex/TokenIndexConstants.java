package io.threesixtyfy.humaneDiscovery.core.tokenIndex;

import org.apache.commons.lang3.StringUtils;

public class TokenIndexConstants {

    public static final String TOKEN_INDEX_TYPE = "token";

    public static class Fields {

        public static final String ENCODINGS = "encodings";
        public static final String KEY = "key";
        public static final String TOKENS = "tokens";
        public static final String POSITION = "position";
        public static final String TOKEN = "token";
        public static final String TOKEN_TYPE = "tokenType";
        public static final String WEIGHT = "weight";
        public static final String TOTAL_COUNT = "totalCount";
        public static final String TOTAL_WEIGHT = "totalWeight";
        public static final String TOKEN_COUNT = "tokenCount";
        public static final String NAME = "name";
        public static final String TAG_TYPE = "tagType";
        public static final String REF_TAG_TYPE = "refTagType";
        public static final String NORMALISED_VALUE = "normalisedValue";
        public static final String TAGS = "tags";
        public static final String SCOPES = "scopes";
        public static final String ANCESTORS = "ancestors";
    }

    public static final String TOKEN_STORE_SUFFIX = ":token_store";
    public static final String TOKEN_INDEX_ENABLED_SETTING = "index.token_index_enabled";

    public static final String TOKEN_NESTED_FIELD = "encodings.token";
    public static final String ENCODING_NESTED_FIELD = "encodings.encodings";

    public static String encodingNestedField(String encoding) {
        if (StringUtils.equals(encoding, Encoding.RS_ENCODING)) {
            return Encoding.RS_ENCODING_NESTED_FIELD;
        } else if (StringUtils.equals(encoding, Encoding.DS_ENCODING)) {
            return Encoding.DS_ENCODING_NESTED_FIELD;
        } else if (StringUtils.equals(encoding, Encoding.BM_ENCODING)) {
            return Encoding.BM_ENCODING_NESTED_FIELD;
        } else if (StringUtils.equals(encoding, Encoding.DM_ENCODING)) {
            return Encoding.DM_ENCODING_NESTED_FIELD;
        } else if (StringUtils.equals(encoding, Encoding.NGRAM_ENCODING)) {
            return Encoding.NGRAM_ENCODING_NESTED_FIELD;
        } else if (StringUtils.equals(encoding, Encoding.NGRAM_START_ENCODING)) {
            return Encoding.NGRAM_START_ENCODING_NESTED_FIELD;
        } else if (StringUtils.equals(encoding, Encoding.NGRAM_END_ENCODING)) {
            return Encoding.NGRAM_END_ENCODING_NESTED_FIELD;
        }

        return null;
    }

    public static class Encoding {

        public static final String RS_ENCODING = "rs";
        public static final String RS_ENCODING_NESTED_FIELD = ENCODING_NESTED_FIELD + "." + RS_ENCODING;
        public static final String DS_ENCODING = "ds";
        public static final String DS_ENCODING_NESTED_FIELD = ENCODING_NESTED_FIELD + "." + DS_ENCODING;
        public static final String BM_ENCODING = "bm";
        public static final String BM_ENCODING_NESTED_FIELD = ENCODING_NESTED_FIELD + "." + BM_ENCODING;
        public static final String DM_ENCODING = "dm";
        public static final String DM_ENCODING_NESTED_FIELD = ENCODING_NESTED_FIELD + "." + DM_ENCODING;
        public static final String NGRAM_ENCODING = "ng";
        public static final String NGRAM_ENCODING_NESTED_FIELD = ENCODING_NESTED_FIELD + "." + NGRAM_ENCODING;

        public static final String NGRAM_START_ENCODING = "ng_s";
        public static final String NGRAM_START_ENCODING_NESTED_FIELD = ENCODING_NESTED_FIELD + "." + NGRAM_START_ENCODING;

        public static final String NGRAM_END_ENCODING = "ng_e";
        public static final String NGRAM_END_ENCODING_NESTED_FIELD = ENCODING_NESTED_FIELD + "." + NGRAM_END_ENCODING;
    }

    private static final String METADATA_STORE_SUFFIX = ":metadata_store";
    private static final int METADATA_STORE_SUFFIX_LENGTH = METADATA_STORE_SUFFIX.length();

    public static String tokenIndexName(String metadataIndexName) {
        return metadataIndexName.substring(0, metadataIndexName.length() - METADATA_STORE_SUFFIX_LENGTH) + TOKEN_STORE_SUFFIX;
    }
}

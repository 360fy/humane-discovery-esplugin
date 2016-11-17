package io.threesixtyfy.humaneDiscovery.tokenFilter;

import org.apache.lucene.analysis.TokenStream;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;

public class EdgeGramTokenFilterFactory extends AbstractTokenFilterFactory {

    public static final String NAME = "humane_edgeGram";

    private static final int DEFAULT_MIN_EDGE_GRAM_SIZE = 2;
    private static final int DEFAULT_MAX_EDGE_GRAM_SIZE = 20;

    private static final String MIN_GRAM_SETTING = "min_gram";
    private static final String MAX_GRAM_SETTING = "max_gram";
    private static final String PAYLOAD_SETTING = "payload";
    private static final String PREFIX_SETTING = "prefix";

    private static final String EDGE_GRAM_PREFIX = "e#";

    private final int minGram;

    private final int maxGram;

    private final boolean payload;

    private final String prefix;

    public EdgeGramTokenFilterFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(indexSettings, name, settings);

        this.minGram = settings.getAsInt(MIN_GRAM_SETTING, DEFAULT_MIN_EDGE_GRAM_SIZE);
        this.maxGram = settings.getAsInt(MAX_GRAM_SETTING, DEFAULT_MAX_EDGE_GRAM_SIZE);
        this.payload = settings.getAsBoolean(PAYLOAD_SETTING, false);
        this.prefix = settings.get(PREFIX_SETTING, EDGE_GRAM_PREFIX);
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new EdgeGramTokenFilter(tokenStream, minGram, maxGram, prefix, payload);
    }
}

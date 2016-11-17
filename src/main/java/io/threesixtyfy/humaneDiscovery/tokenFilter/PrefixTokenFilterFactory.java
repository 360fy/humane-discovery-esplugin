package io.threesixtyfy.humaneDiscovery.tokenFilter;

import org.apache.lucene.analysis.TokenStream;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;

public class PrefixTokenFilterFactory extends AbstractTokenFilterFactory {

    public static final String NAME = "prefix";

    private static final String VALUE_SETTING = "value";

    private final String prefix;

    public PrefixTokenFilterFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(indexSettings, name, settings);

        this.prefix = settings.get(VALUE_SETTING, null);
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new PrefixTokenFilter(tokenStream, prefix);
    }
}

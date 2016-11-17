package io.threesixtyfy.humaneDiscovery.tokenFilter;

import org.apache.lucene.analysis.TokenStream;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;

public class HumaneTokenFilterFactory extends AbstractTokenFilterFactory {

    public static final String NAME = "humane";

    public HumaneTokenFilterFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(indexSettings, name, settings);
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new HumaneTokenFilter(tokenStream);
    }
}

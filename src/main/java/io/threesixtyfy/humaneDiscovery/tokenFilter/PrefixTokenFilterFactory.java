package io.threesixtyfy.humaneDiscovery.tokenFilter;

import org.apache.lucene.analysis.TokenStream;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;
import org.elasticsearch.index.settings.IndexSettingsService;

public class PrefixTokenFilterFactory extends AbstractTokenFilterFactory {

    private final String prefix;

    @Inject
    public PrefixTokenFilterFactory(Index index, IndexSettingsService indexSettingsService, @Assisted String name, @Assisted Settings settings) {
        super(index, indexSettingsService.getSettings(), name, settings);

        this.prefix = settings.get("value", null);
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new PrefixTokenFilter(tokenStream, prefix);
    }
}

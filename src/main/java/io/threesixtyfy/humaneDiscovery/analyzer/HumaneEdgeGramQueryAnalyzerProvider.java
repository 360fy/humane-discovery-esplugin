package io.threesixtyfy.humaneDiscovery.analyzer;

import io.threesixtyfy.humaneDiscovery.tokenFilter.PrefixTokenFilterFactory;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.AbstractIndexAnalyzerProvider;
import org.elasticsearch.index.analysis.CustomAnalyzer;
import org.elasticsearch.index.analysis.LowerCaseTokenFilterFactory;
import org.elasticsearch.index.analysis.StandardTokenizerFactory;
import org.elasticsearch.index.analysis.StopTokenFilterFactory;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.settings.IndexSettingsService;

public class HumaneEdgeGramQueryAnalyzerProvider extends AbstractIndexAnalyzerProvider<CustomAnalyzer> {

    private final CustomAnalyzer customAnalyzer;

    @Inject
    public HumaneEdgeGramQueryAnalyzerProvider(Index index, IndexSettingsService indexSettingsService, Environment env, @Assisted String name, @Assisted Settings settings) {
        super(index, indexSettingsService.indexSettings(), name, settings);
        customAnalyzer = new CustomAnalyzer(new StandardTokenizerFactory(index, indexSettingsService, name, settings),
                null,
                new TokenFilterFactory[]{
                        new LowerCaseTokenFilterFactory(index, indexSettingsService, name, settings),
//                        new StopTokenFilterFactory(index, indexSettingsService, env, name, settings),
                        new PrefixTokenFilterFactory(index, indexSettingsService, name,
                                Settings.builder().put("value", "e#")
                                        .build())
                });
    }

    @Override
    public CustomAnalyzer get() {
        return this.customAnalyzer;
    }
}

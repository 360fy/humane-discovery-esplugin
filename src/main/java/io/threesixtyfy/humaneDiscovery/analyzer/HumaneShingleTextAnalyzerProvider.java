package io.threesixtyfy.humaneDiscovery.analyzer;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractIndexAnalyzerProvider;
import org.elasticsearch.index.analysis.CustomAnalyzer;
import org.elasticsearch.index.analysis.LowerCaseTokenFilterFactory;
import org.elasticsearch.index.analysis.ShingleTokenFilterFactory;
import org.elasticsearch.index.analysis.StandardTokenizerFactory;
import org.elasticsearch.index.analysis.TokenFilterFactory;

public class HumaneShingleTextAnalyzerProvider extends AbstractIndexAnalyzerProvider<CustomAnalyzer> {

    public static final String NAME = "humane_shingle_text_analyzer";

    private static final String TOKEN_SEPARATOR_DEFAULT = "";
    private static final boolean OUTPUT_UNIGRAM_DEFULT = false;

    private final CustomAnalyzer customAnalyzer;

    public HumaneShingleTextAnalyzerProvider(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(indexSettings, name, settings);
        customAnalyzer = new CustomAnalyzer(new StandardTokenizerFactory(indexSettings, environment, name, settings),
                null,
                new TokenFilterFactory[]{
                        new LowerCaseTokenFilterFactory(indexSettings, environment, name, settings),
                        new ShingleTokenFilterFactory(indexSettings, environment, name,
                                Settings.builder()
                                        .put(AnalyzerConstants.OUTPUT_UNIGRAM_SETTING, OUTPUT_UNIGRAM_DEFULT)
                                        .put(AnalyzerConstants.TOKEN_SEPARATOR_SETTING, TOKEN_SEPARATOR_DEFAULT)
                                        .build())
                });
    }

    @Override
    public CustomAnalyzer get() {
        return this.customAnalyzer;
    }
}

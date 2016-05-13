package io.threesixtyfy.humaneDiscovery.plugin;

import com.google.common.collect.ImmutableList;
import io.threesixtyfy.humaneDiscovery.analyzer.HumaneEdgeGramQueryAnalyzerProvider;
import io.threesixtyfy.humaneDiscovery.analyzer.HumaneKeywordAnalyzerProvider;
import io.threesixtyfy.humaneDiscovery.analyzer.HumaneQueryAnalyzerProvider;
import io.threesixtyfy.humaneDiscovery.analyzer.HumaneShingleTextAnalyzerProvider;
import io.threesixtyfy.humaneDiscovery.analyzer.HumaneStandardAnalyzerProvider;
import io.threesixtyfy.humaneDiscovery.analyzer.HumaneTextAnalyzerProvider;
import io.threesixtyfy.humaneDiscovery.analyzer.HumaneVernacularAnalyzerProvider;
import io.threesixtyfy.humaneDiscovery.didYouMean.action.DidYouMeanAction;
import io.threesixtyfy.humaneDiscovery.didYouMean.action.TransportDidYouMeanAction;
import io.threesixtyfy.humaneDiscovery.didYouMean.builder.DidYouMeanBuilderService;
import io.threesixtyfy.humaneDiscovery.didYouMean.rest.DidYouMeanRestAction;
import io.threesixtyfy.humaneDiscovery.query.HumaneQueryParser;
import io.threesixtyfy.humaneDiscovery.query.MultiFieldHumaneQueryParser;
import io.threesixtyfy.humaneDiscovery.tokenFilter.EdgeGramTokenFilterFactory;
import io.threesixtyfy.humaneDiscovery.tokenFilter.HumaneTokenFilterFactory;
import io.threesixtyfy.humaneDiscovery.tokenFilter.PrefixTokenFilterFactory;
import org.elasticsearch.action.ActionModule;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.index.analysis.AnalysisModule;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestModule;

import java.util.Collection;

public class HumaneDiscoveryPlugin extends Plugin {

    @Override
    public String name() {
        return "humane_discovery";
    }

    @Override
    public String description() {
        return "humane discovery";
    }

    public void onModule(IndicesModule module) {
        module.registerQueryParser(HumaneQueryParser.class);
        module.registerQueryParser(MultiFieldHumaneQueryParser.class);
    }

    public void onModule(AnalysisModule module) {
        module.addTokenFilter("humane", HumaneTokenFilterFactory.class);
        module.addTokenFilter("prefix", PrefixTokenFilterFactory.class);
        module.addTokenFilter("humane_edgeGram", EdgeGramTokenFilterFactory.class);

        module.addAnalyzer("humane_text_analyzer", HumaneTextAnalyzerProvider.class);
        module.addAnalyzer("humane_shingle_text_analyzer", HumaneShingleTextAnalyzerProvider.class);
        module.addAnalyzer("humane_query_analyzer", HumaneQueryAnalyzerProvider.class);
        module.addAnalyzer("humane_edgeGram_query_analyzer", HumaneEdgeGramQueryAnalyzerProvider.class);
        module.addAnalyzer("humane_standard_analyzer", HumaneStandardAnalyzerProvider.class);
        module.addAnalyzer("humane_keyword_analyzer", HumaneKeywordAnalyzerProvider.class);
        module.addAnalyzer("humane_vernacular_analyzer", HumaneVernacularAnalyzerProvider.class);
    }

    public void onModule(RestModule module) {
        module.addRestAction(DidYouMeanRestAction.class);
    }

    public void onModule(ActionModule module) {
        module.registerAction(DidYouMeanAction.INSTANCE, TransportDidYouMeanAction.class);
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> nodeServices() {
        return ImmutableList.of(DidYouMeanBuilderService.class);
    }
}

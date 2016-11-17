package io.threesixtyfy.humaneDiscovery.plugin;

import io.threesixtyfy.humaneDiscovery.analyzer.HumaneDescriptiveTextAnalyzerProvider;
import io.threesixtyfy.humaneDiscovery.analyzer.HumaneEdgeGramQueryAnalyzerProvider;
import io.threesixtyfy.humaneDiscovery.analyzer.HumaneKeywordAnalyzerProvider;
import io.threesixtyfy.humaneDiscovery.analyzer.HumaneQueryAnalyzerProvider;
import io.threesixtyfy.humaneDiscovery.analyzer.HumaneShingleTextAnalyzerProvider;
import io.threesixtyfy.humaneDiscovery.analyzer.HumaneStandardAnalyzerProvider;
import io.threesixtyfy.humaneDiscovery.analyzer.HumaneTextAnalyzerProvider;
import io.threesixtyfy.humaneDiscovery.analyzer.HumaneVernacularAnalyzerProvider;
import io.threesixtyfy.humaneDiscovery.api.didYouMean.DidYouMeanAction;
import io.threesixtyfy.humaneDiscovery.api.didYouMean.DidYouMeanRestAction;
import io.threesixtyfy.humaneDiscovery.api.didYouMean.TransportDidYouMeanAction;
import io.threesixtyfy.humaneDiscovery.api.intent.IntentAction;
import io.threesixtyfy.humaneDiscovery.api.intent.IntentRestAction;
import io.threesixtyfy.humaneDiscovery.api.intent.TransportIntentAction;
import io.threesixtyfy.humaneDiscovery.query.HumaneQueryBuilder;
import io.threesixtyfy.humaneDiscovery.query.MultiHumaneQueryBuilder;
import io.threesixtyfy.humaneDiscovery.service.wordIndex.IndexEventListenerImpl;
import io.threesixtyfy.humaneDiscovery.service.wordIndex.IndexingOperationListenerImpl;
import io.threesixtyfy.humaneDiscovery.service.wordIndex.LifecycleService;
import io.threesixtyfy.humaneDiscovery.service.wordIndex.SharedChannel;
import io.threesixtyfy.humaneDiscovery.service.wordIndex.WordIndexConstants;
import io.threesixtyfy.humaneDiscovery.tokenFilter.EdgeGramTokenFilterFactory;
import io.threesixtyfy.humaneDiscovery.tokenFilter.HumaneTokenFilterFactory;
import io.threesixtyfy.humaneDiscovery.tokenFilter.PrefixTokenFilterFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.analysis.AnalyzerProvider;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchRequestParsers;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HumaneDiscoveryPlugin extends Plugin implements AnalysisPlugin, ActionPlugin, SearchPlugin {

    private static final Logger logger = Loggers.getLogger(HumaneDiscoveryPlugin.class);

    private final SharedChannel sharedChannel;

    public HumaneDiscoveryPlugin() {
        this.sharedChannel = new SharedChannel();
    }

    public void onIndexModule(IndexModule indexModule) {
        logger.info("onIndexModule for: {}", indexModule.getIndex().getName());

        if (indexModule.getSettings().getAsBoolean(WordIndexConstants.WORD_INDEX_ENABLED_SETTING, Boolean.FALSE)) {
            logger.info("Adding index event listener for: {}", indexModule.getIndex().getName());
            indexModule.addIndexEventListener(new IndexEventListenerImpl(indexModule.getIndex(), this.sharedChannel));

            logger.info("Adding index operation listener for: {}", indexModule.getIndex().getName());
            try {
                Analyzer analyzer = this.getAnalyzers().get(HumaneStandardAnalyzerProvider.NAME).get(null, HumaneStandardAnalyzerProvider.NAME).get();
                indexModule.addIndexOperationListener(new IndexingOperationListenerImpl(indexModule.getIndex(), this.sharedChannel, analyzer));
            } catch (IOException e) {
                logger.error("IOException({}) in retrieving analyzer {} for index {}", e.getMessage(), HumaneStandardAnalyzerProvider.NAME, indexModule.getIndex().getName());
            }
        }
    }

    @Override
    public Map<String, AnalysisModule.AnalysisProvider<TokenFilterFactory>> getTokenFilters() {
        Map<String, AnalysisModule.AnalysisProvider<TokenFilterFactory>> map = new HashMap<>();

        map.put(HumaneTokenFilterFactory.NAME, HumaneTokenFilterFactory::new);
        map.put(PrefixTokenFilterFactory.NAME, PrefixTokenFilterFactory::new);
        map.put(EdgeGramTokenFilterFactory.NAME, EdgeGramTokenFilterFactory::new);

        return map;
    }

    @Override
    public Map<String, AnalysisModule.AnalysisProvider<AnalyzerProvider<? extends Analyzer>>> getAnalyzers() {
        Map<String, AnalysisModule.AnalysisProvider<AnalyzerProvider<? extends Analyzer>>> map = new HashMap<>();

        map.put(HumaneTextAnalyzerProvider.NAME, HumaneTextAnalyzerProvider::new);
        map.put(HumaneDescriptiveTextAnalyzerProvider.NAME, HumaneDescriptiveTextAnalyzerProvider::new);
        map.put(HumaneShingleTextAnalyzerProvider.NAME, HumaneShingleTextAnalyzerProvider::new);
        map.put(HumaneQueryAnalyzerProvider.NAME, HumaneQueryAnalyzerProvider::new);
        map.put(HumaneEdgeGramQueryAnalyzerProvider.NAME, HumaneEdgeGramQueryAnalyzerProvider::new);
        map.put(HumaneStandardAnalyzerProvider.NAME, HumaneStandardAnalyzerProvider::new);
        map.put(HumaneKeywordAnalyzerProvider.NAME, HumaneKeywordAnalyzerProvider::new);
        map.put(HumaneVernacularAnalyzerProvider.NAME, HumaneVernacularAnalyzerProvider::new);

        return map;
    }

    @Override
    public List<QuerySpec<?>> getQueries() {
        List<QuerySpec<?>> querySpecs = new ArrayList<>(2);

        querySpecs.add(new QuerySpec<>(HumaneQueryBuilder.NAME, HumaneQueryBuilder::new, HumaneQueryBuilder::fromXContent));
        querySpecs.add(new QuerySpec<>(MultiHumaneQueryBuilder.NAME, MultiHumaneQueryBuilder::new, MultiHumaneQueryBuilder::fromXContent));

        return querySpecs;
    }

    @Override
    public List<ActionHandler<? extends ActionRequest<?>, ? extends ActionResponse>> getActions() {
        List<ActionHandler<? extends ActionRequest<?>, ? extends ActionResponse>> actions = new ArrayList<>(2);

        actions.add(new ActionHandler<>(DidYouMeanAction.INSTANCE, TransportDidYouMeanAction.class));
        actions.add(new ActionHandler<>(IntentAction.INSTANCE, TransportIntentAction.class));

        return actions;
    }

    @Override
    public List<Class<? extends RestHandler>> getRestHandlers() {
        List<Class<? extends RestHandler>> restHandlers = new ArrayList<>(2);

        restHandlers.add(IntentRestAction.class);
        restHandlers.add(DidYouMeanRestAction.class);

        return restHandlers;
    }

    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool, ResourceWatcherService resourceWatcherService, ScriptService scriptService, SearchRequestParsers searchRequestParsers) {
        return Collections.singletonList(new LifecycleService(sharedChannel, clusterService.getSettings(), client));
    }

    @Override
    public List<Setting<?>> getSettings() {
        List<Setting<?>> settings = new ArrayList<>();
        settings.add(Setting.boolSetting(WordIndexConstants.WORD_INDEX_ENABLED_SETTING, false, Setting.Property.IndexScope));

        return settings;
    }
}

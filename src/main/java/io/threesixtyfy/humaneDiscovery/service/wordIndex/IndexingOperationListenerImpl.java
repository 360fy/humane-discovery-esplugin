package io.threesixtyfy.humaneDiscovery.service.wordIndex;

import io.threesixtyfy.humaneDiscovery.core.encoding.EdgeGramEncodingUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.StringFieldMapper;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.shard.IndexingOperationListener;

import java.io.IOException;

public class IndexingOperationListenerImpl implements IndexingOperationListener {

    private static final String NAME_FIELD = "name";
    private static final String VALUE_HUMANE_FIELD = "value.humane";

    private static final Logger logger = Loggers.getLogger(IndexingOperationListenerImpl.class);

    private final SharedChannel sharedChannel;

    private final Analyzer humaneAnalyzer;
    private final String indexName;

    public IndexingOperationListenerImpl(Index index, SharedChannel sharedChannel, Analyzer humaneAnalyzer) {
        this.sharedChannel = sharedChannel;
        this.humaneAnalyzer = humaneAnalyzer;
        this.indexName = index.getName() + WordIndexConstants.WORD_INDEX_STORE_SUFFIX;
    }

    private void add(ParsedDocument parsedDocument) {
        EdgeGramEncodingUtils edgeGramEncodingUtils = new EdgeGramEncodingUtils(1);

        TokenStream tokenStream = null;

        for (ParseContext.Document document : parsedDocument.docs()) {
            tokenStream = addDoc(edgeGramEncodingUtils, tokenStream, document);
        }
    }

    private TokenStream addDoc(EdgeGramEncodingUtils edgeGramEncodingUtils, TokenStream tokenStream, ParseContext.Document document) {
        IndexableField nameField = document.getField(NAME_FIELD);
        IndexableField valueField = document.getField(VALUE_HUMANE_FIELD);

        if (valueField == null) {
            return tokenStream;
        }

        IndexableFieldType fieldType = valueField.fieldType();

        double suggestionWeight;

        if (fieldType instanceof StringFieldMapper.StringFieldType) {
            suggestionWeight = 2.0;
        } else if (fieldType instanceof TextFieldMapper.TextFieldType) {
            suggestionWeight = 1.0;
        } else {
            return tokenStream;
        }

        tokenStream = valueField.tokenStream(this.humaneAnalyzer, tokenStream);

        String fieldName = nameField.stringValue();

        try {
            tokenStream.reset();
            CharTermAttribute termAttribute = tokenStream.getAttribute(CharTermAttribute.class);

            String previousWord = null;

            while (tokenStream.incrementToken()) {
                String word = termAttribute.toString();

                sharedChannel.getWordQueue().offer(new WordInfo(this.indexName, fieldName, word, suggestionWeight, false, word));

                if (StringUtils.isNotEmpty(previousWord)) {
                    sharedChannel.getWordQueue().offer(new BigramWordInfo(this.indexName, fieldName, previousWord, word, suggestionWeight, false, previousWord + word, previousWord + " " + word));
                }

                for (String edgeGram : edgeGramEncodingUtils.buildEncodings(word)) {
                    if (fieldType instanceof StringFieldMapper.StringFieldType || edgeGram.length() > 1) {
                        sharedChannel.getWordQueue().offer(new WordInfo(this.indexName, fieldName, edgeGram, suggestionWeight, true, word));
                    }

                    if (StringUtils.isNotEmpty(previousWord)) {
                        sharedChannel.getWordQueue().offer(new BigramWordInfo(this.indexName, fieldName, previousWord, edgeGram, suggestionWeight, true, previousWord + word, previousWord + " " + word));
                    }
                }

                previousWord = word;
            }

            tokenStream.close();
        } catch (IOException e) {
            logger.error("IOException in parsing document {}, name {}", e, document, valueField);
        }

        return tokenStream;
    }

    @Override
    // TODO: differentiate between update and create
    public void postIndex(final Engine.Index index, boolean created) {
        add(index.parsedDoc());
    }

    @Override
    // TODO: remove values on delete
    public void postDelete(final Engine.Delete delete) {
        // do nothing for now
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        IndexingOperationListenerImpl that = (IndexingOperationListenerImpl) o;

        return indexName.equals(that.indexName);

    }

    @Override
    public int hashCode() {
        return indexName.hashCode();
    }
}

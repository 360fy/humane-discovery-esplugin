package io.threesixtyfy.humaneDiscovery.core.tokenIndex;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.shard.IndexingOperationListener;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static io.threesixtyfy.humaneDiscovery.core.tokenIndex.TokenIndexConstants.tokenIndexName;

public class IndexingOperationListenerImpl implements IndexingOperationListener {

    private static final String NAME_FIELD = "name";
    private static final String VALUE_HUMANE_FIELD = "value.humane";

    private static final String STOP_WORD_TYPE = "stopWord";
    private static final String KEYWORD_TYPE = "keyword";
    private static final String INTENT_TYPE = "intent";

    private static final Logger logger = Loggers.getLogger(IndexingOperationListenerImpl.class);
    private static final String ANCESTORS_FIELD = "ancestors";

    private final SharedChannel sharedChannel;

    private final Analyzer humaneAnalyzer;
    private final String indexName;

    public IndexingOperationListenerImpl(Index index, SharedChannel sharedChannel, Analyzer humaneAnalyzer) {
        this.sharedChannel = sharedChannel;
        this.humaneAnalyzer = humaneAnalyzer;
        this.indexName = tokenIndexName(index.getName());
    }

    private void add(ParsedDocument parsedDocument) {
        TokenStream tokenStream = null;

        String type = parsedDocument.type();

        for (ParseContext.Document document : parsedDocument.docs()) {
            tokenStream = addDoc(tokenStream, type, document);
        }
    }

    private String getValue(ParseContext.Document document, String field) {
        IndexableField indexableField = document.getField(field);
        if (indexableField == null) {
            return null;
        }

        return indexableField.stringValue();
    }

    private List<String> singletonList(String value) {
        List<String> list = new ArrayList<>();
        list.add(value);

        return list;
    }

    private List<List<String>> ancestors(ParseContext.Document document) {
        IndexableField[] fields = document.getFields(ANCESTORS_FIELD);
        if (fields != null && fields.length > 0) {
            List<List<String>> ancestors = new ArrayList<>();

            for (IndexableField field : fields) {
                ancestors.add(singletonList(field.stringValue()));
            }

            return ancestors;
        }

        return null;
    }

    private List<List<String>> copyAncestors(List<List<String>> ancestors, String ancestor) {
        List<List<String>> newAncestors = null;
        if (ancestors != null) {
            newAncestors = new ArrayList<>();
            newAncestors.add(singletonList(ancestor));
            newAncestors.addAll(ancestors);
        }

        return newAncestors;
    }

    private boolean invalidUnigram(String token) {
        return NumberUtils.isParsable(token) || token.length() <= 1;
    }

    private boolean invalidBigram(String token1, String token2) {
        return invalidUnigram(token1) && invalidUnigram(token2);
    }

    private TokenStream addDoc(TokenStream tokenStream, String type, ParseContext.Document document) {
        IndexableField valueField = document.getField(VALUE_HUMANE_FIELD);

        if (valueField == null) {
            return tokenStream;
        }

        tokenStream = valueField.tokenStream(this.humaneAnalyzer, tokenStream);

        String name = getValue(document, NAME_FIELD);

        try {
            tokenStream.reset();
            CharTermAttribute termAttribute = tokenStream.getAttribute(CharTermAttribute.class);

            List<String> tokenList = new ArrayList<>();

            while (tokenStream.incrementToken()) {
                String word = termAttribute.toString();

                tokenList.add(word);
            }

            tokenStream.close();

            // if type == intent, then fetch ancestors too
            // for type == intent, store the intent value
            if (StringUtils.equals(type, INTENT_TYPE)) {
                List<List<String>> ancestors = ancestors(document);

                sharedChannel.getTokenQueue().offer(TokenInfo.intentToken(this.indexName, tokenList, name, ancestors));

                // if size == 1, then do not add ngramToken
                // if size == 2, then do not add bigramToken
                // if type = keyword / stopWord, then do not add unigram / bigram
                int size = tokenList.size();

                if (size >= 2) {
                    String ancestor = valueField.stringValue();

                    for (int i = 0; i < size; i++) {
                        if (!invalidUnigram(tokenList.get(i))) {
                            sharedChannel.getTokenQueue().offer(TokenInfo.ngramToken(this.indexName, tokenList.subList(i, i + 1), name, copyAncestors(ancestors, ancestor)));
                        }

                        if (size > 2 && i < size - 1 && !invalidBigram(tokenList.get(i), tokenList.get(i + 1))) {
                            sharedChannel.getTokenQueue().offer(TokenInfo.ngramToken(this.indexName, tokenList.subList(i, i + 2), name, copyAncestors(ancestors, ancestor)));
                        }
                    }
                }
            } else if (StringUtils.equals(type, KEYWORD_TYPE)) {
                String normalisedValue = getValue(document, TokenIndexConstants.Fields.NORMALISED_VALUE);
                sharedChannel.getTokenQueue().offer(TokenInfo.keywordToken(this.indexName, tokenList, name, normalisedValue));
            } else if (StringUtils.equals(type, STOP_WORD_TYPE)) {
                sharedChannel.getTokenQueue().offer(TokenInfo.stopWordToken(this.indexName, tokenList, name));
            }
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

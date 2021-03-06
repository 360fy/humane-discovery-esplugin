package io.threesixtyfy.humaneDiscovery.api.intent;

import io.threesixtyfy.humaneDiscovery.api.commons.BaseResult;
import io.threesixtyfy.humaneDiscovery.core.tagForest.MatchLevel;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

// entityClass to array of results for that entity class
public class IntentResult extends BaseResult {

    private Map<String, List<IntentClassResult>> intentClasses;

    // how the display looks like
    private Map<String, String> display;

    public IntentResult() {
    }

    public IntentResult(Map<String, List<IntentClassResult>> intentClasses, double score) {
//        super(score);
        // TODO: maintain score here only
        this.intentClasses = intentClasses;
        this.display = new HashMap<>();
        intentClasses.entrySet().forEach(v -> display.put(v.getKey(), v.getValue().get(0).display));
    }

    public Map<String, List<IntentClassResult>> getIntentClasses() {
        return intentClasses;
    }

    public Map<String, String> getDisplay() {
        return display;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        int numClasses = in.readVInt();

        this.intentClasses = new HashMap<>(numClasses);
        this.display = new HashMap<>();
        for (int i = 0; i < numClasses; i++) {
            String key = in.readString();
            String classDisplay = in.readString();
            int numClassResults = in.readVInt();
            List<IntentClassResult> intentClassResults = new ArrayList<>(numClassResults);
            for (int j = 0; j < numClassResults; j++) {
                IntentClassResult intentClassResult = new IntentClassResult();
                intentClassResult.readFrom(in);
                intentClassResults.add(intentClassResult);
            }

            display.put(key, classDisplay);
            intentClasses.put(key, intentClassResults);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(intentClasses.size());
        for (Map.Entry<String, List<IntentClassResult>> entry : intentClasses.entrySet()) {
            out.writeString(entry.getKey());
            out.writeString(display.get(entry.getKey()));
            out.writeVInt(entry.getValue().size());
            for (IntentClassResult intentClassResult : entry.getValue()) {
                intentClassResult.writeTo(out);
            }
        }
    }

    @Override
    protected void buildXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(Fields.DISPLAY);
        builder.startObject();

        for (Map.Entry<String, String> entry : display.entrySet()) {
            builder.field(entry.getKey(), entry.getValue());
        }

        builder.endObject();
        builder.field(Fields.INTENT_CLASSES);
        builder.startObject();

        for (Map.Entry<String, List<IntentClassResult>> entry : intentClasses.entrySet()) {
            builder.field(entry.getKey());
            builder.startArray();
            for (IntentClassResult intentClassResult : entry.getValue()) {
                intentClassResult.buildXContent(builder, params);
            }
            builder.endArray();
        }

        builder.endObject();
    }

    private static class Fields {
        static final String DISPLAY = "display";
        static final String SCORE = "score";
        static final String TOKEN = "token";
        static final String MATCH_LEVEL = "matchLevel";
        static final String INPUT_TOKEN_TYPE = "inputTokenLength";
        static final String MATCH_TOKEN_TYPE = "matchTokenLength";
        static final String EDIT_DISTANCE = "editDistance";
        static final String INTENT_TOKENS = "intentTokens";
        static final String INTENT_CLASSES = "intentClasses";
    }

    // each entity class display is one suggestion
    public static class IntentClassResult {
        private String intentClass;
        private List<IntentToken> intentTokens;

        // how the display looks like
        private String display;

        private double score;

        public IntentClassResult() {
        }

        public IntentClassResult(String intentClass, List<IntentToken> intentTokens, double score) {
            this.intentClass = intentClass;
            this.intentTokens = intentTokens;
            this.score = score;
            this.display = intentTokens.stream().map(v -> v.display).collect(Collectors.joining(" "));
        }

        public String getIntentClass() {
            return intentClass;
        }

        public List<IntentToken> getIntentTokens() {
            return intentTokens;
        }

        public String getDisplay() {
            return display;
        }

        public double getScore() {
            return score;
        }

        public void readFrom(StreamInput in) throws IOException {
            display = in.readString();
            score = in.readDouble();

            int numTokens = in.readVInt();
            intentTokens = new ArrayList<>();
            for (int i = 0; i < numTokens; i++) {
                IntentToken intentToken = new IntentToken();

                intentToken.readFrom(in);

                intentTokens.add(intentToken);
            }
        }

        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(display);
            out.writeDouble(score);

            out.writeVInt(intentTokens.size());
            for (IntentToken intentToken : intentTokens) {
                intentToken.writeTo(out);
            }
        }

        protected void buildXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Fields.DISPLAY, display);
            builder.field(Fields.SCORE, score);
            builder.field(Fields.INTENT_TOKENS);

            builder.startArray();

            for (IntentToken intentToken : intentTokens) {
                intentToken.buildXContent(builder, params);
            }

            builder.endArray();

            builder.endObject();
        }
    }

    public static class IntentToken {
        private String token;
        private String display;
        private MatchLevel matchLevel;
        private int inputTokenLength;
        private int matchTokenLength;
        private int editDistance;
        private double score;

        public IntentToken() {
        }

        public IntentToken(String token, String display, int inputTokenLength, int matchTokenLength, MatchLevel matchLevel, int editDistance, double score) {
            this.token = token;
            this.display = display;
            this.inputTokenLength = inputTokenLength;
            this.matchTokenLength = matchTokenLength;
            this.matchLevel = matchLevel;
            this.editDistance = editDistance;
            this.score = score;
        }

        public String getToken() {
            return token;
        }

        public String getDisplay() {
            return display;
        }

        public MatchLevel getMatchLevel() {
            return matchLevel;
        }

        public int getInputTokenLength() {
            return inputTokenLength;
        }

        public int getMatchTokenLength() {
            return matchTokenLength;
        }

        public int getEditDistance() {
            return editDistance;
        }

        public double getScore() {
            return score;
        }

        public void readFrom(StreamInput in) throws IOException {
            token = in.readString();
            display = in.readString();
            matchLevel = MatchLevel.valueOf(in.readString());
            inputTokenLength = in.readInt();
            matchTokenLength = in.readInt();
            editDistance = in.readVInt();
            score = in.readDouble();
        }

        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(token);
            out.writeString(display);
            out.writeString(matchLevel.name());
            out.writeInt(inputTokenLength);
            out.writeInt(matchTokenLength);
            out.writeVInt(editDistance);
            out.writeDouble(score);
        }

        protected void buildXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Fields.TOKEN, token);
            builder.field(Fields.DISPLAY, display);
            builder.field(Fields.MATCH_LEVEL, matchLevel);
            builder.field(Fields.INPUT_TOKEN_TYPE, inputTokenLength);
            builder.field(Fields.MATCH_TOKEN_TYPE, matchTokenLength);
            builder.field(Fields.EDIT_DISTANCE, editDistance);
            builder.field(Fields.SCORE, score);
            builder.endObject();
        }
    }
}

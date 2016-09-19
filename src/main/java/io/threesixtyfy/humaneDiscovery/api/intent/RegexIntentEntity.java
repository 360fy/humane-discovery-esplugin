package io.threesixtyfy.humaneDiscovery.api.intent;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

public class RegexIntentEntity extends IntentEntity<RegexIntentEntity> {

    private String regex;

    private Pattern pattern;

    private Set<ResultField> resultFields;

    public String getRegex() {
        return regex;
    }

    public void setRegex(String regex) {
        this.regex = regex;
        this.pattern = Pattern.compile(regex);
    }

    public Pattern getPattern() {
        return pattern;
    }

    public Set<ResultField> getResultFields() {
        return resultFields;
    }

    public void setResultFields(Set<ResultField> resultFields) {
        this.resultFields = resultFields;
    }

    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);

        this.setRegex(in.readString());

        int numResultFields = in.readVInt();
        this.resultFields = new HashSet<>(numResultFields);

        for (int i = 0; i < numResultFields; i++) {
            // read intent entities
            int type = in.readVInt();
            ResultField resultField;
            if (type == 0) {
                // match result field
                resultField = new MatchResultField();
            } else /*if (type == 1) */ {
                // fixed result field
                resultField = new FixedResultField();
            }

            resultField.readFrom(in);
            this.resultFields.add(resultField);
        }
    }

    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        out.writeString(regex);

        out.writeVInt(this.resultFields.size());
        for (ResultField resultField : this.resultFields) {
            // read intent entities
            if (resultField instanceof MatchResultField) {
                // match result field
                out.writeVInt(0);
            } else /*if (type == 1) */ {
                // fixed result field
                out.writeVInt(1);
            }

            resultField.writeTo(out);
        }
    }

    @Override
    public String toString() {
        return "{" +
                "regex='" + regex + '\'' +
                ", resultFields=" + resultFields +
                "} " + super.toString();
    }

    public static abstract class ResultField {

        private String key;

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public void readFrom(StreamInput in) throws IOException {
            this.key = in.readString();
        }

        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(this.key);
        }

        @Override
        public String toString() {
            return "" +
                    "key='" + key + '\'';
        }
    }

    public static class MatchResultField extends ResultField {

        private int matchIndex;

        public int getMatchIndex() {
            return matchIndex;
        }

        public void setMatchIndex(int matchIndex) {
            this.matchIndex = matchIndex;
        }

        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            this.matchIndex = in.readVInt();
        }

        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVInt(this.matchIndex);
        }

        @Override
        public String toString() {
            return "" +
                    "matchIndex=" + matchIndex +
                    " " + super.toString();
        }
    }

    public static class FixedResultField extends ResultField {

        private String value;

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            this.value = in.readString();
        }

        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(this.value);
        }

        @Override
        public String toString() {
            return "" +
                    "value='" + value + '\'' +
                    " " + super.toString();
        }
    }
}

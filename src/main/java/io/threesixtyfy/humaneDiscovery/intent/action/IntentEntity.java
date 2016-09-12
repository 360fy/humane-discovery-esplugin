package io.threesixtyfy.humaneDiscovery.intent.action;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;

public abstract class IntentEntity<T extends IntentEntity> {

    private String name;

    private ResultField[] resultFields;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ResultField[] getResultFields() {
        return resultFields;
    }

    public void setResultFields(ResultField[] resultFields) {
        this.resultFields = resultFields;
    }

    public void readFrom(StreamInput in) throws IOException {
        this.name = in.readString();

        int numResultFields = in.readVInt();
        this.resultFields = new ResultField[numResultFields];

        for (int i = 0; i < numResultFields; i++) {
            // read intent entities
            int type = in.readVInt();
            if (type == 0) {
                // match result field
                this.resultFields[i] = new MatchResultField();
            } else /*if (type == 1) */ {
                // fixed result field
                this.resultFields[i] = new FixedResultField();
            }

            this.resultFields[i].readFrom(in);
        }
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.name);

        out.writeVInt(this.resultFields.length);
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
            return "{" +
                    "key='" + key + '\'' +
                    '}';
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
            return "{" +
                    "matchIndex=" + matchIndex +
                    "} " + super.toString();
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
            return "{" +
                    "value='" + value + '\'' +
                    "} " + super.toString();
        }
    }

    @Override
    public String toString() {
        return "{" +
                "name='" + name + '\'' +
                ", resultFields=" + Arrays.toString(resultFields) +
                '}';
    }
}

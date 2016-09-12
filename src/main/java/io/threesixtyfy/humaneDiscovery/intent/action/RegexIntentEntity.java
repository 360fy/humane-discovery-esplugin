package io.threesixtyfy.humaneDiscovery.intent.action;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.regex.Pattern;

public class RegexIntentEntity extends IntentEntity<RegexIntentEntity> {

    private String regex;

    private Pattern pattern;

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

    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);

        this.setRegex(in.readString());
    }

    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        out.writeString(regex);
    }
}

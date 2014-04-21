package storm.kafka.scheme;

import kafka.message.Message;
import backtype.storm.spout.SchemeAsMultiScheme;
import java.util.Arrays;
import java.util.List;

public class KafkaMessageMultiScheme extends SchemeAsMultiScheme {

    // Store copy of scheme to avoid need for casting
    private KafkaMessageScheme kmScheme;

    public KafkaMessageMultiScheme(KafkaMessageScheme scheme) {
        super(scheme);
        kmScheme = scheme;
    }

    public Iterable<List<Object>> deserialize(final Message message) {
        List<Object> o = kmScheme.deserialize(message);
        if (o == null) {
            return null;
        } else {
            return Arrays.asList(o);
        }
    }
}

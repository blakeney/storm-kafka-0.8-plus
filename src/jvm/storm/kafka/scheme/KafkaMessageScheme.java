package storm.kafka.scheme;

import kafka.message.Message;
import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.util.ArrayList;
import java.util.List;
import java.nio.ByteBuffer;
import java.io.UnsupportedEncodingException;

public class KafkaMessageScheme implements Scheme {

	@Override
	public List<Object> deserialize(byte[] ser) {
		return new Values(deserializeString(ser));
	}

	@Override
    public Fields getOutputFields() {
        return new Fields("key", "payload");
    }
	
	public List<Object> deserialize(Message message) {
		List<Object> deser = new ArrayList<Object>();
		ByteBuffer key = message.key();
        if (key == null) {
        	deser.add(null);            
        }
        else {
        	deser.addAll(deserialize(Utils.toByteArray(key)));
        }
        deser.addAll(deserialize(Utils.toByteArray(message.payload())));
        return deser;
    }
    
    private static String deserializeString(byte[] string) {
        try {
            return new String(string, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
}

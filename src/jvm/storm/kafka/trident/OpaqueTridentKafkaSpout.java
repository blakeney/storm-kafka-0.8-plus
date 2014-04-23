package storm.kafka.trident;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import storm.kafka.Partition;
import storm.trident.spout.IOpaquePartitionedTridentSpout;


public class OpaqueTridentKafkaSpout implements IOpaquePartitionedTridentSpout<GlobalPartitionInformation, Partition, Map> {


    TridentKafkaConfig _config;
    String _topologyInstanceId = UUID.randomUUID().toString();

    public OpaqueTridentKafkaSpout(TridentKafkaConfig config) {
        _config = config;
    }

    @Override
    public IOpaquePartitionedTridentSpout.Emitter<GlobalPartitionInformation, Partition, Map> getEmitter(Map conf, TopologyContext context) {
        try {
            return new TridentKafkaEmitter(conf, context, _config, _topologyInstanceId).asOpaqueEmitter();
        } catch (IOException e) {
            throw new RuntimeException("Unable to connect to ZooKeeper", e);
        }
    }

    @Override
    public IOpaquePartitionedTridentSpout.Coordinator getCoordinator(Map conf, TopologyContext tc) {
        try {
            return new storm.kafka.trident.Coordinator(conf, _config);
        } catch (IOException e) {
            throw new RuntimeException("Unable to connect to ZooKeeper", e);
        }
    }

    @Override
    public Fields getOutputFields() {
        return _config.scheme.getOutputFields();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}

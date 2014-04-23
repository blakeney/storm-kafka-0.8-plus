package storm.kafka.trident;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import storm.kafka.Partition;
import storm.trident.spout.IPartitionedTridentSpout;


public class TransactionalTridentKafkaSpout implements IPartitionedTridentSpout<GlobalPartitionInformation, Partition, Map> {

    TridentKafkaConfig _config;
    String _topologyInstanceId = UUID.randomUUID().toString();

    public TransactionalTridentKafkaSpout(TridentKafkaConfig config) {
        _config = config;
    }


    @Override
    public IPartitionedTridentSpout.Coordinator getCoordinator(Map conf, TopologyContext context) {
        try {
            return new storm.kafka.trident.Coordinator(conf, _config);
        } catch (IOException e) {
            throw new RuntimeException("Unable to connect to ZooKeeper", e);
        }
    }

    @Override
    public IPartitionedTridentSpout.Emitter getEmitter(Map conf, TopologyContext context) {
        try {
            return new TridentKafkaEmitter(conf, context, _config, _topologyInstanceId).asTransactionalEmitter();
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
package consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

public class MyConsumerRebalancerListener implements ConsumerRebalanceListener {
    private OffsetManager offsetManager =new OffsetManager("D:\\ExternalStorage\\");
    private Consumer<String,String> consumer;

    public MyConsumerRebalancerListener(KafkaConsumer<String, String> consumer) {
        this.consumer=consumer;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
        for(TopicPartition partition:collection){
            offsetManager.saveOffsetInExternalStorage(partition.topic(),partition.partition(),consumer.position(partition));
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
        for(TopicPartition partition:collection){
            consumer.seek(partition,offsetManager.readOffsetFromExternalStorage(partition.topic(),partition.partition()));
        }
    }
}

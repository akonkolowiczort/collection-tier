package uy.edu.ort.arqrealtime.collection.tier;

import org.apache.kafka.clients.producer.*;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

final public class RSVPKafkaProducer implements RSVPProducer {
    private static KafkaProducer<byte[], byte[]> kafkaProducer;
    private static final String messageTopic = "meetup-raw-rsvps";

    public RSVPKafkaProducer(){
        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProperties.put(ProducerConfig.CLIENT_ID_CONFIG, "meetup-collection-service-kafka");
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArraySerializer");

        kafkaProducer = new KafkaProducer<>(producerProperties);
    }


    public void sendMessage(final String messageKey, final byte[] message) {

        ProducerRecord<byte[],byte[]> producerRecord = new ProducerRecord<>(messageTopic,
                messageKey.getBytes(StandardCharsets.UTF_8),message);
        kafkaProducer.send(producerRecord,new TopicCallbackHandler(messageKey));
    }

    public void close() {
        kafkaProducer.close();
    }


    private final class TopicCallbackHandler implements Callback {
        final String eventKey;

        TopicCallbackHandler(final String eventKey){
            this.eventKey = eventKey;
        }

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (null == metadata){
                //mark record as failed
                HybridMessageLogger.moveToFailed(eventKey);
            }else{
                //remove the data from the localstate
                try {
                    HybridMessageLogger.removeEvent(eventKey);
                } catch (Exception e) {
                    //this should be logged...
                }
            }

        }
    }
}

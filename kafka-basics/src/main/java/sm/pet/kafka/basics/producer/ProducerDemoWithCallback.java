package sm.pet.kafka.basics.producer;

import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import sm.pet.kafka.basics.utils.Utils;

@Slf4j
public class ProducerDemoWithCallback {


    public static void main(String[] args) {
        Properties properties = Utils.getProducerProperties();

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        ProducerRecord<String, String> record = new ProducerRecord<>("demo_java", "hello!");

        Callback callback = (metadata, exception) -> {
            if (exception == null) {
                log.info("Received new metadata: " +
                    " topic: " + metadata.topic() +
                    " partition: " + metadata.partition() +
                    " offset: " + metadata.offset() +
                    " timestamp: " + metadata.timestamp());
            } else {
                log.error("Error while producing: ", exception);
            }
        };

        for (int i = 0; i < 10; i++) {
            producer.send(record, callback);
        }

        //flush, block and close
        producer.close();
    }
}

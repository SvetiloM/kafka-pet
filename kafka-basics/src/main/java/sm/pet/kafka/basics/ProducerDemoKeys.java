package sm.pet.kafka.basics;

import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import sm.pet.kafka.basics.utils.Utils;

@Slf4j
public class ProducerDemoKeys {


    public static void main(String[] args) {
        Properties properties = Utils.getProperties();

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        String topic = "demo_java";

        for (int j = 0; j < 2; j++) {
            for (int i = 0; i < 10; i++) {
                String key = "id_" + i;
                String value = "hello " + i;
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

                Callback callback = (metadata, exception) -> {
                    if (exception == null) {
                        log.info("Received new metadata: " +
                            " key: " + key +
                            " partition: " + metadata.partition());
                    } else {
                        log.error("Error while producing: ", exception);
                    }
                };

                producer.send(record, callback);
            }
        }

        //flush, block and close
        producer.close();
    }
}

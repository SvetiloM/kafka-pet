package sm.pet.kafka.basics;

import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import sm.pet.kafka.basics.utils.Utils;

@Slf4j
public class ProducerDemo {


    public static void main(String[] args) {
        Properties properties = Utils.getProperties();

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        ProducerRecord<String, String> record = new ProducerRecord<>("demo_java", "hello!");

        producer.send(record);

        //flush, block and close
        producer.close();
    }
}

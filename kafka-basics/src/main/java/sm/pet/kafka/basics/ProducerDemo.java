package sm.pet.kafka.basics;

import java.util.Properties;
import lombok.extern.java.Log;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

@Log
public class ProducerDemo {

    //todo add localhost
    public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    public static final String SECURITY_PROTOCOL = "security.protocol";
    public static final String SASL_JAAS_CONFIG = "sasl.jaas.config";
    public static final String SASL_MECHANISM = "sasl.mechanism";

    public static void main(String[] args) {
        Properties properties = new Properties();

        properties.setProperty(BOOTSTRAP_SERVERS, System.getenv(BOOTSTRAP_SERVERS));
        properties.setProperty(SECURITY_PROTOCOL, System.getenv(SECURITY_PROTOCOL));
        properties.setProperty(SASL_JAAS_CONFIG, System.getenv(SASL_JAAS_CONFIG));
        properties.setProperty(SASL_MECHANISM, System.getenv(SASL_MECHANISM));

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        ProducerRecord<String, String> record = new ProducerRecord<>("demo_java", "hello!");

        producer.send(record);

        //flush, block and close
        producer.close();
    }
}

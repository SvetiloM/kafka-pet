package sm.pet.kafka.basics.utils;

import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_MECHANISM;

import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class Utils {

    public static Properties getProducerProperties() {
        Properties properties = new Properties();

        enRichConnection(properties);
        enrichProducerProperties(properties);

        return properties;
    }

    public static Properties getConsumerProperties() {
        Properties properties = new Properties();

        enRichConnection(properties);
        enrichConsumerProperties(properties);

        return properties;
    }

    private static void enRichConnection(Properties properties) {
        if (System.getenv(SECURITY_PROTOCOL_CONFIG) != null) {
            properties.setProperty(SECURITY_PROTOCOL_CONFIG, System.getenv(SECURITY_PROTOCOL_CONFIG));
            properties.setProperty(SASL_JAAS_CONFIG, System.getenv(SASL_JAAS_CONFIG));
            properties.setProperty(SASL_MECHANISM, System.getenv(SASL_MECHANISM));
        }

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    }

    private static void enrichProducerProperties(Properties properties) {
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    }

    private static void enrichConsumerProperties(Properties properties) {
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    }

}

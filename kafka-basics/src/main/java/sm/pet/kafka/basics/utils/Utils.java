package sm.pet.kafka.basics.utils;

import java.util.Properties;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class Utils {

    private static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    private static final String SECURITY_PROTOCOL = "security.protocol";
    private static final String SASL_JAAS_CONFIG = "sasl.jaas.config";
    private static final String SASL_MECHANISM = "sasl.mechanism";
    private static final String KEY_SERIALIZER = "key.serializer";
    private static final String VALUE_SERIALIZER = "value.serializer";
    private static final String KEY_DESERIALIZER = "key.deserializer";
    private static final String VALUE_DESERIALIZER = "value.deserializer";

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
        if (System.getenv(SECURITY_PROTOCOL) != null) {
            properties.setProperty(SECURITY_PROTOCOL, System.getenv(SECURITY_PROTOCOL));
            properties.setProperty(SASL_JAAS_CONFIG, System.getenv(SASL_JAAS_CONFIG));
            properties.setProperty(SASL_MECHANISM, System.getenv(SASL_MECHANISM));
        }

        properties.setProperty(BOOTSTRAP_SERVERS, System.getenv(BOOTSTRAP_SERVERS));
    }

    private static void enrichProducerProperties(Properties properties) {
        properties.setProperty(KEY_SERIALIZER, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER, StringSerializer.class.getName());
    }

    private static void enrichConsumerProperties(Properties properties) {
        properties.setProperty(KEY_DESERIALIZER, StringDeserializer.class.getName());
        properties.setProperty(VALUE_DESERIALIZER, StringDeserializer.class.getName());
    }

}

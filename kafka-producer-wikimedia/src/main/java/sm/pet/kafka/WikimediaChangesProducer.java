package sm.pet.kafka;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import sm.pet.kafka.basics.utils.Utils;

public class WikimediaChangesProducer {

    public static void main(String[] args) throws InterruptedException {
        Properties producerProperties = Utils.getProducerProperties();
        producerProperties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        producerProperties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));
        producerProperties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);

        EventHandler handler = new WikimediaChangeHandler(producer, System.getenv("wikimedia.topic"));

        EventSource.Builder builder =  new EventSource.Builder(handler, URI.create(System.getenv("wikimedia.url")));
        EventSource eventSource = builder.build();

        // start the producer in another thread
        eventSource.start();

        // we produce for 10 minutes and block the program until then
        TimeUnit.MINUTES.sleep(10);
    }

}

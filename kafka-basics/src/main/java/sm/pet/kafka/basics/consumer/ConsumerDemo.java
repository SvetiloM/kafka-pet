package sm.pet.kafka.basics.consumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import sm.pet.kafka.basics.utils.Utils;

@Slf4j
public class ConsumerDemo {

    public static void main(String[] args) {
        String groupId = "my-java-application";
        String topic = "demo_java";
        Properties properties = Utils.getConsumerProperties();

        properties.setProperty("group.id", groupId);
        //none - if we don't have any existing group - we will fail
        //earliest - read topic from the beginning
        //latest - just start reading right now ignoring all existing messages
        properties.setProperty("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Collections.singleton(topic));

        while (true) {

            log.info("Polling...");

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

            for (ConsumerRecord<String, String> record : records) {
                log.info("key: " + record.key()
                    + " value: " + record.value()
                    + " partition: " + record.partition()
                    + " offset: " + record.offset());
            }
        }

    }
}

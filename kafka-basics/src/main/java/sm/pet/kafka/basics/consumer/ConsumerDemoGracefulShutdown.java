package sm.pet.kafka.basics.consumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import sm.pet.kafka.basics.utils.Utils;

@Slf4j
public class ConsumerDemoGracefulShutdown {

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

        final Thread mainThread = Thread.currentThread();

        //will start when VM starts its shutdown process
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                log.info("Detected a shutdown...");
                consumer.wakeup(); //will abort long poll and throw a WakeUpEx

                try {
                    //let it execute in the main thread
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
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
        } catch (WakeupException e) {
            log.info("Consumer is starting to shutdown");
        } catch (Exception e) {
            log.info("We shouldn't be here");
        } finally {
            consumer.close();
            log.info("We closed the consumer and commited the offset");
        }

    }
}

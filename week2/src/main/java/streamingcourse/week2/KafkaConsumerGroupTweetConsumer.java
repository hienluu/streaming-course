package streamingcourse.week2;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * This example shows how to start multiple consumers for a consumer group.
 * Each consumer runs in its own thread.
 *
 * The number of consumers is controlled by the numConsumers
 *
 * Observe the output to see the message from each consumer id.  If
 *
 *
 */
public class KafkaConsumerGroupTweetConsumer {
    private static final String BOOTSTRAP_SERVER_LIST = "localhost:9092,localhost:9093,localhost:9094";
    private static final String KAFKA_TOPIC_TO_CONSUME_FROM = "streaming.week2.tweets";
    private static final String GROUP_ID = KafkaConsumerGroupTweetConsumer.class.getName();


    public static void main(String[] args) {
        System.out.println(KafkaConsumerGroupTweetConsumer.class.getName());

        int numConsumers = 3;
        boolean start_from_beginning = false;

        System.out.println(" =======================  consumer information =========================");
        System.out.println("Consuming group: " + GROUP_ID + " subscribe to topic: " + KAFKA_TOPIC_TO_CONSUME_FROM);
        System.out.println("spinning up: " + numConsumers + " consumers");
        System.out.println("start_from_beginning: " + start_from_beginning);
        System.out.println(" =======================  consumer information =========================");


        Properties props = createKafkaProperties();

        ExecutorService executorService = Executors.newFixedThreadPool(numConsumers);
        List<KafkaTweetConsumer> consumerList = new ArrayList<>(numConsumers);

        for (int i = 1; i <= numConsumers; i++) {
            KafkaTweetConsumer consumer = new KafkaTweetConsumer(props, i, Duration.ofMillis(150),
                    Collections.singletonList(KAFKA_TOPIC_TO_CONSUME_FROM));
            consumerList.add(consumer);
        }

        for (KafkaTweetConsumer consumer : consumerList) {
            executorService.submit(consumer);
        }

        // setup the shutdown hook to close the consumers properly
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.out.println(" ^^^^^^ shutdown hook - closing consumers ^^^^^^^");
                for (KafkaTweetConsumer consumer : consumerList) {
                    consumer.shutdown();
                }
                executorService.shutdown();
                try {
                    executorService.awaitTermination(5000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                System.out.println(" ^^^^^^ shutdown hook done ^^^^^^^");
            }
        });
    }

    private static Properties createKafkaProperties() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_LIST);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    /**
     * A Kafka consumer wrapped in a thread
     */
    private static class KafkaTweetConsumer implements Runnable {
        private final KafkaConsumer<String,String> consumer;
        private final int consumerId;
        private final List<String> topicList;
        private final Duration timeout;


        public KafkaTweetConsumer(Properties props, int consumerId, Duration timeout, List<String> topicList) {
            this.consumerId = consumerId;
            this.topicList = topicList;
            this.timeout = timeout;

            consumer = new KafkaConsumer<String, String>(props);
        }

        @Override
        public void run() {
            try {
                consumer.subscribe(topicList);

                while (true) {
                    ConsumerRecords<String, String> messages = consumer.poll(timeout);

                    for (ConsumerRecord<String, String> msg : messages) {
                        System.out.printf("consumer: %d, partition: %d, offset: %d, key: %s, value: %s\n",
                                consumerId,  msg.partition(), msg.offset(), msg.key(), msg.value());
                    }
                }
            } catch (WakeupException e) {
                // ignore for shutdown
                System.out.println(consumerId + " got WakeupException");
            } finally {
                consumer.close();
            }
        }

        public void shutdown() {
            consumer.wakeup();
        }
    }

}

package streamingcourse.week2.kafkamessaging;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import streamingcourse.common.KafkaCommonProperties;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static streamingcourse.week2.PrintColorCode.*;
import static streamingcourse.common.KafkaCommonProperties.*;

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
    private static final String BOOTSTRAP_SERVER_LIST = KafkaCommonProperties.BOOTSTRAP_SERVER_LIST;
    private static final String KAFKA_TOPIC_TO_CONSUME_FROM = TWEETS_TOPIC_NAME;
    private static final String GROUP_ID = KafkaConsumerGroupTweetConsumer.class.getName();


    public static void main(String[] args) {
        System.out.println(KafkaConsumerGroupTweetConsumer.class.getName());

        int numConsumers = 3;
        boolean start_from_beginning = true;

        System.out.println("=======================  consumer information =========================");
        System.out.println(" Consuming group: " + GROUP_ID + " subscribe to topic: " + KAFKA_TOPIC_TO_CONSUME_FROM);
        System.out.println(" Spinning up: " + numConsumers + " consumers");
        System.out.println(" start_from_beginning: " + start_from_beginning);
        System.out.println("=======================  consumer information =========================");

        Properties props = createKafkaProperties();

        ExecutorService executorService = Executors.newFixedThreadPool(numConsumers);
        List<KafkaTweetConsumer> consumerList = new ArrayList<>(numConsumers);

        for (int i = 1; i <= numConsumers; i++) {
            KafkaTweetConsumer consumer = new KafkaTweetConsumer(props, i, Duration.ofMillis(150),
                    Collections.singletonList(KAFKA_TOPIC_TO_CONSUME_FROM), start_from_beginning);
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
        private final boolean from_beginning;

        private final String ansiColor;
        private static final int retry_count = 5;


        public KafkaTweetConsumer(Properties props, int consumerId, Duration timeout, List<String> topicList,
                                  boolean from_beginning) {
            this.consumerId = consumerId;
            this.topicList = topicList;
            this.timeout = timeout;
            this.from_beginning = from_beginning;
            this.ansiColor = getAnsiColor(consumerId);

            consumer = new KafkaConsumer<String, String>(props);
        }

        @Override
        public void run() {
            try {
                consumer.subscribe(topicList);

                if (from_beginning) {
                    resetPartitionOffset(retry_count, timeout);
                }

                while (true) {
                    ConsumerRecords<String, String> messages = consumer.poll(timeout);

                    for (ConsumerRecord<String, String> msg : messages) {
                        System.out.printf("%s consumer: %d, partition: %d, offset: %d, key: %s, value: %s\n",
                                ansiColor ,consumerId,  msg.partition(), msg.offset(), msg.key(), msg.value());
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

        private void resetPartitionOffset(int retry_count, Duration waitDuration) {
            System.out.printf("--- consumer: %d resetPartitionOffset with retry count of %d and wait duration of  %d ms ---\n",
                    consumerId, retry_count, waitDuration.toMillis());
            // trigger the connection to Kafka partition follower in order to retrieve assignment information
            Duration partitionRetrievalWaitDuration = waitDuration;
            consumer.poll(partitionRetrievalWaitDuration);
            Set<TopicPartition> topicPartitionSet = consumer.assignment();
            if (topicPartitionSet.isEmpty()) {
                for (int i = 0; i < retry_count; i++) {
                    consumer.poll(partitionRetrievalWaitDuration);
                    topicPartitionSet = consumer.assignment();
                    if (!topicPartitionSet.isEmpty()) {
                        break;
                    }
                }
                if (topicPartitionSet.isEmpty()) {
                    System.out.printf("consumer: %d Exhausted: %d retries and unsuccessfully " +
                            "retrieve the partition assignment information\n", consumerId, retry_count);
                }
            }
            System.out.printf("consumer: %d Found: %d partitions\n", consumerId, topicPartitionSet.size());
            topicPartitionSet.forEach(partition -> {
                System.out.printf("consumer: %d partition: %s\n", consumerId, partition.toString());

            });
            // seek for the beginning of the provided partitions
            consumer.seekToBeginning(topicPartitionSet);

            System.out.println();
        }
    }

}

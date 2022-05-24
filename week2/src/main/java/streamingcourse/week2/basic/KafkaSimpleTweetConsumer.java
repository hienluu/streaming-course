package streamingcourse.week2.basic;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;


import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;

import static streamingcourse.week2.PrintColorCode.*;
import static streamingcourse.week2.KafkaCommonProperties.*;

/**
 * A simple Kafka consumer to demonstrate how to read messages from Kafka cluster
 *
 * - When you run this for the very first time, time offset doesn't exist on the Kafka broker side,
 *   therefore it will read messages from the beginning of the topic.
 * - To stop the consumer, click on the stop button
 * - when you run this for the subsequent time, you won't see any messages, why?
 */
public class KafkaSimpleTweetConsumer {
    private static final String BOOTSTRAP_SERVER_LIST = "localhost:9092,localhost:9093,localhost:9094";
    private static final String KAFKA_TOPIC_TO_CONSUME_FROM = TWEETS_TOPIC_NAME;
    private static final String GROUP_ID = KafkaSimpleTweetConsumer.class.getName();


    public static void main(String[] args) {
        System.out.println(KafkaSimpleTweetConsumer.class.getName());

        Properties props = createKafkaProperties();

        boolean start_from_beginning = true;

        System.out.println(" =======================  consumer information =========================");
        System.out.println("Consuming group: " + GROUP_ID + " subscribe to topic: " + KAFKA_TOPIC_TO_CONSUME_FROM);
        System.out.println("start_from_beginning: " + start_from_beginning);
        System.out.println(" =======================  consumer information =========================");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Collections.singletonList(KAFKA_TOPIC_TO_CONSUME_FROM));

        int retry_count = 5;
        if (start_from_beginning) {
            resetPartitionOffset(consumer, retry_count, Duration.ofMillis(200));
        } else {
            System.out.printf("**** Didn't seek to the beginning of the partitions ****\n");
        }

        Duration timeout = Duration.ofMillis(250);
        try {
            System.out.println("*** going into the poll loop ***");
            while (true) {
                ConsumerRecords<String, String> messages = consumer.poll(timeout);


                for (ConsumerRecord<String, String> msg : messages) {
                    System.out.printf("%s topic: %s, partition: %d, offset: %d, key: %s, value: %s\n",
                            ansiBlue(), msg.topic(), msg.partition(), msg.offset(), msg.key(), msg.value());
                }
            }
        }  catch (Exception e) {
            // no-op
        } finally {
            consumer.close();
        }
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

    private static void resetPartitionOffset(KafkaConsumer consumer, int retry_count, Duration waitDuration) {
        System.out.printf("--- resetPartitionOffset with retry count of %d and wait duration of  %d ms ---",
                retry_count, waitDuration.toMillis());
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
                System.out.println("Exhausted: " + retry_count + " retries and unsuccessfully " +
                        "retrieve the partition assignment information");
            }
        }
        System.out.println("Found: " + topicPartitionSet.size() + " partitions");
        topicPartitionSet.forEach(partition -> {
            System.out.println(partition);
        });
        // seek for the beginning of the provided partitions
        consumer.seekToBeginning(topicPartitionSet);

        System.out.println();
    }
}

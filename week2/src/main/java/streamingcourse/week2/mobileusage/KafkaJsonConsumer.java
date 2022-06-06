package streamingcourse.week2.mobileusage;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import streamingcourse.common.KafkaCommonProperties;
import streamingcourse.week2.common.MyJsonSerializer;
import streamingcourse.week2.kafkamessaging.KafkaSimpleTweetConsumer;
import streamingcourse.week2.kafkamessaging.MobileUsageDeserializer;
import streamingcourse.week2.mobileusage.data.MobileUsageProducer;
import streamingcourse.week2.mobileusage.model.MobileUsage;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;

import static streamingcourse.week2.PrintColorCode.ansiGreen;
import static streamingcourse.week2.PrintColorCode.ansiYellow;

public class KafkaJsonConsumer {
     private static final String BOOTSTRAP_SERVER_LIST = KafkaCommonProperties.BOOTSTRAP_SERVER_LIST;
    private static final String KAFKA_TOPIC_TO_CONSUME_FROM = MobileUsageProducer.MOBILE_USAGE_TOPIC;
    private static final String GROUP_ID = KafkaJsonConsumer.class.getName();

    private static Logger log = LogManager.getLogger(KafkaJsonConsumer.class.getName());

    public static void main(String[] args) {
        System.out.println(KafkaSimpleTweetConsumer.class.getName());

        Properties props = createKafkaProperties();

        boolean start_from_beginning = true;
        boolean printWithDetail = false;

        log.info(" =======================  consumer information =========================");
        log.info("Consuming group: " + GROUP_ID + " subscribe to topic: " + KAFKA_TOPIC_TO_CONSUME_FROM);
        log.info("start_from_beginning: " + start_from_beginning);
        log.info(" =======================  consumer information =========================");

        KafkaConsumer<String, MobileUsage> consumer = new KafkaConsumer<String, MobileUsage>(props);
        consumer.subscribe(Collections.singletonList(KAFKA_TOPIC_TO_CONSUME_FROM));

        int retry_count = 5;
        if (start_from_beginning) {
            resetPartitionOffset(consumer, retry_count, Duration.ofMillis(500));
        } else {
            System.out.printf("**** Didn't seek to the beginning of the partitions ****\n");
        }

        Duration timeout = Duration.ofMillis(250);
        try {
            MyJsonSerializer myJsonSerializer = MyJsonSerializer.build();

            System.out.println("*** going into the poll loop ***");
            while (true) {
                ConsumerRecords<String, MobileUsage> messages = consumer.poll(timeout);

                for (ConsumerRecord<String, MobileUsage> msg : messages) {
                    String valueInJson = myJsonSerializer.toJson(msg.value());
                    if (printWithDetail) {
                        System.out.printf("%s topic: %s, partition: %d, offset: %d, record: %s:%s\n",
                                ansiYellow(), msg.topic(), msg.partition(), msg.offset(), msg.key(), valueInJson);
                    } else {
                        System.out.printf("%s%s:%s\n",
                                ansiGreen(), msg.key(), valueInJson);

                    }
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
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MobileUsageDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    /**
     * For each partition, reset the partition offset to the beginning.  The partition assignment metadata
     * is retrieved from the broker.
     *
     * @param consumer
     * @param retry_count
     * @param waitDuration
     */
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

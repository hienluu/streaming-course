package streamingcourse.week2.json;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import streamingcourse.week2.kafkastreams.KStreamDisplay;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static streamingcourse.week2.KafkaCommonProperties.BOOTSTRAP_SERVER_LIST;

public class MobileUsageDisplay {
    public static final String MOBILE_USAGE_TOPIC_NAME = MobileUsageProducer.KAFKA_TOPIC_TO_SEND_TO;

    private static Logger log = LogManager.getLogger(KStreamDisplay.class.getName());
    public static void main(final String[] args) throws Exception {
        log.info("============== MobileUsageDisplay.main ============= ");
        log.info("reading lines from:  " + MOBILE_USAGE_TOPIC_NAME);
        log.info("===================================================== ");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "MobileUsageDisplay-app");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "MobileUsageDisplay-client");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_LIST);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, MobileUsageSerde.class);
        //props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        // Note: To re-run the demo, you need to use the offset reset tool:
        // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Records should be flushed every 10 seconds. This is less than the default
        // in order to keep this example interactive.
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        // For illustrative purposes we disable record caches.
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);


        StreamsBuilder builder = new StreamsBuilder();
        // create a stream from the WORD_COUNT_INPUT_TOPIC_NAME
        KStream<String, MobileUsage> lineStream = builder.stream(MOBILE_USAGE_TOPIC_NAME);

        /*
        log.info("======== peeking ========");
        lineStream.peek((key, value) -> log.info(String.format("key: %s, value: %s", key, value.userName)));

         */


        log.info("======== group by user ========");
        lineStream.groupBy((key, value) -> value.userName).count().toStream().peek(
                (key, value) -> log.info(String.format("key: %s, value: %s", key, value))
        );

        Topology topology = builder.build();
        log.info("topology: "  + topology.describe().toString());
        KafkaStreams streams = new KafkaStreams(topology, props);

        // reset the Kafka Streams app. state
        log.info("Performing clean up");
        streams.cleanUp();

        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                log.info("Shutdown called..");
                streams.close();
                latch.countDown();
            }
        });

        // Now run the processing topology via `start()` to begin processing its input data.
        log.info("Start running the topology");
        streams.start();
        latch.await();

    }
}

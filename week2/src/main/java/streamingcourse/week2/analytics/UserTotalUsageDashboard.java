package streamingcourse.week2.analytics;

import com.influxdb.annotations.Column;
import com.influxdb.annotations.Measurement;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import streamingcourse.common.influxdb.InfluxdbClient;
import streamingcourse.week2.kafkastreams.stateful.AverageUserUsedBytesSerdes;
import streamingcourse.week2.mobileusage.data.MobileUsageProducer;
import streamingcourse.week2.mobileusage.model.MobileUsage;
import streamingcourse.week2.mobileusage.serdes.MobileUseCaseAppSerdes;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static streamingcourse.common.KafkaCommonProperties.BOOTSTRAP_SERVER_LIST;

/**
 * This is an end to end example of computing the user total bytes usage and send that
 * data to influxdb
 */
public class UserTotalUsageDashboard {
    private static Logger log = LogManager.getLogger(UserTotalUsageDashboard.class.getName());

    private static String MOBILE_USAGE_TOPIC_NAME = MobileUsageProducer.MOBILE_USAGE_TOPIC;


    public static void main(final String[] args) throws Exception {
        log.info("============== UserTotalUsageDashboard.main ============= ");
        log.info("reading messages from topic:  " + MOBILE_USAGE_TOPIC_NAME);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, UserTotalUsageDashboard.class.getSimpleName() + "-app");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, UserTotalUsageDashboard.class.getSimpleName() + "-client");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_LIST);


        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        // Note: To re-run the demo, you need to use the offset reset tool:
        // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Records should be flushed every 10 seconds. This is less than the default
        // in order to keep this example interactive.
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        // For illustrative purposes we disable record caches.
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        log.info("APPLICATION_ID_CONFIG:  " + props.get(StreamsConfig.APPLICATION_ID_CONFIG));

        // influxdb client
        InfluxdbClient influxdbClient = new InfluxdbClient();
        influxdbClient.printProperties();;

        StreamsBuilder builder = new StreamsBuilder();

        Serde<MobileUsage> mobileUsageSerde = MobileUseCaseAppSerdes.MobileUsage();
        KStream<String, MobileUsage> mobileUsageStream = builder.stream(MOBILE_USAGE_TOPIC_NAME,
                Consumed.with(Serdes.String(), mobileUsageSerde)
        );

        KTable<String, Long> totalUsageByUserTable = mobileUsageStream.groupByKey()
                .aggregate(() -> 0L, (key, value, aggValue) -> aggValue += value.bytesUsed,
                        Materialized.with(Serdes.String(), Serdes.Long())
                );

        totalUsageByUserTable.toStream().foreach((key, value) -> {
            UseTotalMobileUsage usage = new UseTotalMobileUsage();
            usage.userName = key;
            usage.totalBytesUsed = value;
            usage.time = Instant.now();

            influxdbClient.writeMeasurement(usage);

            log.info(String.format("%s:%d", key, value));
        });

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
                log.info("Shutdown called..closing the streams");
                streams.close();
                streams.cleanUp();
                influxdbClient.close();
                latch.countDown();
            }
        });

        // Now run the processing topology via `start()` to begin processing its input data.
        log.info("Start running the topology");
        streams.start();
        latch.await();

    }

    @Measurement(name = "mobileusage")
    public static class UseTotalMobileUsage {
        @Column(tag = true)
        String userName;
        @Column
        Long totalBytesUsed;
        @Column(timestamp = true)
        Instant time;

        @Override
        public String toString() {
            return "UseTotalMobileUsage{" +
                    "userName='" + userName + '\'' +
                    ", bytesUsed=" + totalBytesUsed +
                    ", time=" + time +
                    '}';
        }
    }

}

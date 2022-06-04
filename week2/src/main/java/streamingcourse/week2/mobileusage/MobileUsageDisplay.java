package streamingcourse.week2.mobileusage;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import streamingcourse.week2.kafkastreams.KStreamDisplay;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static streamingcourse.common.KafkaCommonProperties.BOOTSTRAP_SERVER_LIST;

/**
 * Example of performing streaming analytics of the mobile usage data
 *
 * Resource:
 * - When encountered class cast exception when using supress()
 *   https://peaku.co/questions/29703-kafkastreams:-obtencion-de-los-resultados-finales-de-la-ventana
 */
public class MobileUsageDisplay {
    public static final String MOBILE_USAGE_TOPIC_NAME = MobileUsageProducer.KAFKA_TOPIC_TO_SEND_TO;

    private static Logger log = LogManager.getLogger(KStreamDisplay.class.getName());

    private static void displayMobileUsageRecords(KStream<String, MobileUsage> lineStream) {
        log.info("======== peeking ========");
       // lineStream.peek((key, value) -> log.info(String.format("key: %s, value: %s", key, value)));
        lineStream.print(Printed.<String, MobileUsage>toSysOut().withLabel("Mobile Usage"));
    }

    private static void displayCountByUser(KStream<String, MobileUsage> lineStream) {
        log.info("======== group by user, then count ========");
        lineStream.groupByKey().count().toStream().peek(
                (key, value) -> log.info(String.format("key: %s, value: %s", key, value))
        );
    }


    /**
     * Display total mobile usage by user using the aggregate function
     * @param lineStream
     */
    private static void displayTotalMobileUsageByUserUsingAggregate(KStream<String, MobileUsage> lineStream) {
        log.info("======== display total mobile usage by user using aggregate function ========");
        lineStream.groupByKey()
                .aggregate(() -> 0.0,
                        (key,value, total) -> total + value.getBytesUsed(),
                    Materialized.with(Serdes.String(), Serdes.Double()))
                .toStream()
                .print(Printed.<String,Double>toSysOut().withLabel("user usage"));

                //peek((key, value) -> log.info(String.format("key: %s, value: %s", key, value))
       // );
    }

    private static void displayMobileUsageLeaderboard(KStream<String, MobileUsage> lineStream) {
        Initializer<TopMobileUsage> highScoresInitializer = TopMobileUsage::new;

        Aggregator<String, TotalMobileUsage, TopMobileUsage> highScoresAdder =
                (key, value, aggregate) -> aggregate.add(value);
    }


    private static void displayCountByUserWithWindow(KStream<String, MobileUsage> lineStream, long windowSizeInSecond) {

        TimeWindows tumblingWindow =
                TimeWindows.of(Duration.ofSeconds(windowSizeInSecond)).grace(Duration.ZERO);
        log.info("======== group by user w/ tumbling window: " + tumblingWindow + " ========");

        final Serde<String> stringSerde = Serdes.String();
        KTable<Windowed<String>, Long> userCount = lineStream
                .groupByKey()
                .windowedBy(tumblingWindow)
                .count(Materialized.with(Serdes.String(), Serdes.Long()))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig
                        .unbounded().shutDownWhenFull()))
                ;

        userCount.toStream().peek(
                (key, value) -> log.info(String.format("window: %s-%s key: %s, value: %s",
                        key.window().startTime(),
                        key.window().endTime(),
                        key.key(),
                        value))
        );

    }
    public static void main(final String[] args) throws Exception {
        log.info("============== MobileUsageDisplay.main ============= ");
        log.info("reading lines from:  " + MOBILE_USAGE_TOPIC_NAME);
        log.info("===================================================== ");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "MobileUsageDisplay-app");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "MobileUsageDisplay-client");
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
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        MobileUsageTimeExtractor timeExtractor = new MobileUsageTimeExtractor();

        StreamsBuilder builder = new StreamsBuilder();

        MobileUsageSerde mobileUsageSerde = new MobileUsageSerde();
        Serde<MobileUsage> mobileUsageSerde2 = MobileUsageAppSerdes.MobileUsage();
        KStream<String, MobileUsage> lineStream = builder.stream(MOBILE_USAGE_TOPIC_NAME,
                Consumed.with(Serdes.String(), mobileUsageSerde2)
                //Consumed.with(Serdes.String(), mobileUsageSerde)
                       .withTimestampExtractor(timeExtractor)
                );


      //displayMobileUsageRecords(lineStream);

      //  displayCountByUser(lineStream);
       //displayCountByUserWithWindow(lineStream, TimeUnit.SECONDS.toSeconds(60));

       displayTotalMobileUsageByUserUsingAggregate(lineStream);

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
                latch.countDown();
            }
        });

        // Now run the processing topology via `start()` to begin processing its input data.
        log.info("Start running the topology");
        streams.start();
        latch.await();

    }
}

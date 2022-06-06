package streamingcourse.week2.leaderboard;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import streamingcourse.week2.mobileusage.data.MobileUsageProducer;
import streamingcourse.week2.mobileusage.model.MobileUsage;
import streamingcourse.week2.mobileusage.serdes.MobileUseCaseAppSerdes;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static streamingcourse.common.KafkaCommonProperties.BOOTSTRAP_SERVER_LIST;

/**
 * Computing top n users with highest mobile usage
 * - group by user and aggregate the usage
 * - group by common key to track only top n users by usaage
 *
 */
public class TopNUserLeaderboard {
    private static Logger log = LogManager.getLogger(TopNUserLeaderboard.class.getName());

    private static String MOBILE_USAGE_TOPIC_NAME = MobileUsageProducer.MOBILE_USAGE_TOPIC;
        private static String LEADER_STATE_STORE_NAME = "topNUser-leader-board";

    public static void main(final String[] args) throws Exception {
        log.info("============== TopNUserLeaderboard.main ============= ");
        log.info("reading messages from topic:  " + MOBILE_USAGE_TOPIC_NAME);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,  TopNUserLeaderboard.class.getSimpleName() +  "-app");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, TopNUserLeaderboard.class.getSimpleName() + "-client");
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

        StreamsBuilder builder = new StreamsBuilder();

        Serde<MobileUsage> mobileUsageSerde = MobileUseCaseAppSerdes.MobileUsage();
        KStream<String, MobileUsage> mobileUsageStream = builder.stream(MOBILE_USAGE_TOPIC_NAME,
                Consumed.with(Serdes.String(), mobileUsageSerde)
        );

        /**
         * ============== main aggregation logic goes here ===========
         */


        // group by username and use aggregate to sum up their usage
        Initializer<UserTotalUsage> mobileUsageInitializer  = UserTotalUsage::new;
        Aggregator<String, MobileUsage, UserTotalUsage> mobileUsageAggregator =
                (aggKey,newValue, aggValue) -> {
                    aggValue.userName = aggKey;
                    aggValue.increaseBy(newValue.getBytesUsed());
                    aggValue.deptId = newValue.getDeptId();
                    return aggValue;
                };

        Serde<UserTotalUsage> userTotalUsageSerdes = TopNUserSerdes.UserTotalUsage();
        KTable<String, UserTotalUsage> totalUsageByUser = mobileUsageStream.groupByKey()
                .aggregate(
                        mobileUsageInitializer,
                        mobileUsageAggregator,
                        Materialized.with(Serdes.String(), userTotalUsageSerdes)
                );

        // group by a common key across all users to figure who the to n users area
        Initializer<TopNUsers> topNUsersInitializer  = () -> new TopNUsers(3);
        Aggregator<String, UserTotalUsage, TopNUsers> topNUsersAggregator = (aggKey, newValue, aggValue) -> aggValue.add(newValue);
        Aggregator<String, UserTotalUsage, TopNUsers> topNUsersSubtractor = (aggKey, oldValue, aggValue) -> aggValue.remove(oldValue);


        KTable<String, TopNUsers> topNUserTable = totalUsageByUser.groupBy((userName, totalUsage) ->
             KeyValue.pair("topNUser", totalUsage),
            Grouped.with(Serdes.String(), userTotalUsageSerdes)
        ).aggregate(
                // initializer
                topNUsersInitializer,
                // aggregator
                topNUsersAggregator,
                // subtractor
                topNUsersSubtractor,
                Materialized.<String, TopNUsers, KeyValueStore <Bytes, byte[]>>as(LEADER_STATE_STORE_NAME)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(TopNUserSerdes.TopNUsers())
        );

        // now display the top n users
        topNUserTable.toStream().foreach((key, value) -> {
            if (value != null) {
                System.out.printf("%s:%s\n", key, value.getTop3Sorted());
            }
        });


        Topology topology = builder.build();
        log.info("topology: "  + topology.describe().toString());
        KafkaStreams streams = new KafkaStreams(topology, props);

        StoreQueryServer queryServer = new StoreQueryServer(streams, LEADER_STATE_STORE_NAME, 7777);
        streams.setStateListener((newState, oldState) -> {
            log.info("State Changing to " + newState + " from " + oldState);
            queryServer.setActive(newState == KafkaStreams.State.RUNNING && oldState == KafkaStreams.State.REBALANCING);
        });

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
                queryServer.stop();
                latch.countDown();
            }
        });

        // Now run the processing topology via `start()` to begin processing its input data.
        log.info("Start running the topology");
        streams.start();
        queryServer.start();
        latch.await();

    }

}

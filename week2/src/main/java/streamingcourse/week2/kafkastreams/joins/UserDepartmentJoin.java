package streamingcourse.week2.kafkastreams.joins;

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
import streamingcourse.week2.common.MobileUsageCommonProperties;
import streamingcourse.week2.mobileusage.data.DepartmentInfoProducer;
import streamingcourse.week2.mobileusage.data.MobileUsageProducer;
import streamingcourse.week2.mobileusage.model.DeptInfo;
import streamingcourse.week2.mobileusage.model.MobileUsage;
import streamingcourse.week2.mobileusage.serdes.MobileUseCaseAppSerdes;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static streamingcourse.common.KafkaCommonProperties.BOOTSTRAP_SERVER_LIST;

/**
 * Simple example of demonstrating KStream-KGlobalTable join between mobile usage and department info.
 * Since the number of departments is small, we will model it as a KGlobalTable, which doesn't require
 * it to have the same number of partitions as the mobile usage topic.
 *
 * Once the records between these two topics are joined, they will be printed in the console to view.
 * This is used an example to show how to perform a join
 *
 */
public class UserDepartmentJoin {
    private static Logger log = LogManager.getLogger(UserDepartmentJoin.class.getName());

    private static String MOBILE_USAGE_TOPIC_NAME = MobileUsageCommonProperties.MOBILE_USAGE_TOPIC;
    private static String DEPARTMENT_INFO_TOPIC_NAME = DepartmentInfoProducer.DEPARTMENT_TOPIC_NAME;

    public static void main(final String[] args) throws Exception {
        log.info("============== UserDepartmentJoin.main ============= ");
        log.info("reading messages from mobile usage topic:  " + MOBILE_USAGE_TOPIC_NAME);
        log.info("reading messages from department info topic:  " + DEPARTMENT_INFO_TOPIC_NAME);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, UserDepartmentJoin.class.getSimpleName() + "-app");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, UserDepartmentJoin.class.getSimpleName() + "-client");
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

        // mobile usage
        Serde<MobileUsage> mobileUsageSerde = MobileUseCaseAppSerdes.MobileUsage();
        KStream<String, MobileUsage> mobileUsageStream = builder.stream(MOBILE_USAGE_TOPIC_NAME,
                Consumed.with(Serdes.String(), mobileUsageSerde)
        );

        // department info
        Serde<DeptInfo> deptInfoSerde = MobileUseCaseAppSerdes.DepartmentInfo();
        GlobalKTable<Integer, DeptInfo> deptInfoGlobalTable = builder.globalTable(DEPARTMENT_INFO_TOPIC_NAME,
                Consumed.with(Serdes.Integer(), deptInfoSerde));

        // perform the join
        // KeyValueMapper
        KeyValueMapper<String, MobileUsage, Integer> keyValueMapper = (userName, mobileUsage) -> mobileUsage.deptId;
        // ValueJoiner
        ValueJoiner<MobileUsage, DeptInfo, String> valueJoiner = (mobileUsage, deptInfo) -> {
            return mobileUsage.toString() + " ==> " + deptInfo.toString();
        };

        // see javadoc for more details on the behavior and input arguments
        // https://kafka.apache.org/26/javadoc/org/apache/kafka/streams/kstream/KStream.html#join-org.apache.kafka.streams.kstream.GlobalKTable-org.apache.kafka.streams.kstream.KeyValueMapper-org.apache.kafka.streams.kstream.ValueJoiner-
        KStream<String, String> mobileUsageWithDeptInfoGlobalTable =  mobileUsageStream.join(deptInfoGlobalTable,
                keyValueMapper,
                valueJoiner);

        mobileUsageWithDeptInfoGlobalTable.foreach((key, value) -> {
            System.out.printf("%s:%s\n", key, value);
        });

        // start executing the topology
        Topology topology = builder.build();
        log.info("topology: " + topology.describe().toString());
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
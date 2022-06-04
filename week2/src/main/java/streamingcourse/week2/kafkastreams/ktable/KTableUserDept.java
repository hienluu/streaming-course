package streamingcourse.week2.kafkastreams.ktable;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import java.util.Properties;

import static streamingcourse.common.KafkaCommonProperties.BOOTSTRAP_SERVER_LIST;


/**
 * A simple example of demonstrating the behavior of a KTable, which maintains one value per key.
 *
 * This example computes the size of each dept.
 */
public class KTableUserDept {
    public static final String INPUT_TOPIC_NAME = "streaming.week2.user-dept";

    private static void useKTable(StreamsBuilder builder) {
        log.info("============= using KTable example ==========");
        KTable<String,String> userDeptKTable = builder.table(INPUT_TOPIC_NAME);
        userDeptKTable.groupBy((userId, dept) ->KeyValue.pair(dept, "1"),
                        Grouped.with(Serdes.String(), Serdes.String()))
                .count()
                .toStream().print(Printed.<String,Long>toSysOut().withLabel("KTable"));
    }

    private static void useKStream(StreamsBuilder builder) {
        log.info("============= using KStream example ==========");
        KStream<String,String> userDeptStream = builder.stream(INPUT_TOPIC_NAME);
        userDeptStream.groupBy((userId, dept) -> dept,
                        Grouped.with(Serdes.String(), Serdes.String()))
                .count()
                .toStream().print(Printed.<String,Long>toSysOut().withLabel("KStream"));
    }

    
    private static Logger log = LogManager.getLogger(KTableUserDept.class.getName());
    public static void main(final String[] args) throws Exception {
        log.info("==============" + KTableUserDept.class.getName() + " .main ============= ");
        log.info("reading lines from:  " + INPUT_TOPIC_NAME);
        log.info("===================================================== ");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, KTableUserDept.class.getName() + "-app");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, KTableUserDept.class.getName() + " -client");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_LIST);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // Records should be flushed every 10 seconds. This is less than the default
        // in order to keep this example interactive.
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        // For illustrative purposes we disable record caches.
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder builder = new StreamsBuilder();

        boolean useKTable = true;
        if (useKTable) {
            useKTable(builder);
        } else {
            useKStream(builder);
        }
        
        Topology topology = builder.build();
        log.info("topology: "  + topology.describe().toString());
        KafkaStreams streams = new KafkaStreams(topology, props);

        // reset the Kafka Streams app. state
        log.info("Performing clean up");
        streams.cleanUp();

        // Now run the processing topology via `start()` to begin processing its input data.
        log.info("Start running the topology");
        streams.start();

        // shutdown hook to properly and gracefully close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}

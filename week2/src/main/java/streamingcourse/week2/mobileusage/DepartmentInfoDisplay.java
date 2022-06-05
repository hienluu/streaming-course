package streamingcourse.week2.mobileusage;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import streamingcourse.week2.mobileusage.data.DepartmentInfoProducer;
import streamingcourse.week2.mobileusage.model.DeptInfo;
import streamingcourse.week2.mobileusage.serdes.MobileUsageAppSerdes;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static streamingcourse.common.KafkaCommonProperties.BOOTSTRAP_SERVER_LIST;


public class DepartmentInfoDisplay {
    public static final String DEPARTMENT_TOPIC_NAME = DepartmentInfoProducer.KAFKA_TOPIC_TO_SEND_TO;

    private static Logger log = LogManager.getLogger(DepartmentInfoDisplay.class.getName());

    public static void main(final String[] args) throws Exception {
        log.info("============== DepartmentInfoDisplay.main ============= ");
        log.info("reading messages from topic:  " + DEPARTMENT_TOPIC_NAME);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, DepartmentInfoDisplay.class.getSimpleName() +  "-app");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, DepartmentInfoDisplay.class.getSimpleName() + "-client");
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

        Serde<DeptInfo> deptInfoSerde = MobileUsageAppSerdes.DepartmentInfo();
        KStream<Integer, DeptInfo> deptInfoKStream = builder.stream(DEPARTMENT_TOPIC_NAME,
                Consumed.with(Serdes.Integer(), deptInfoSerde)
        );

        deptInfoKStream.print(Printed.<Integer, DeptInfo>toSysOut().withLabel("Department Info"));


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

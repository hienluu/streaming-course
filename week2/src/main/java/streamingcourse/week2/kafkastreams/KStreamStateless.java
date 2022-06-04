package streamingcourse.week2.kafkastreams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Printed;

import java.util.Properties;

import static streamingcourse.common.KafkaCommonProperties.BOOTSTRAP_SERVER_LIST;

/**
 * Examples of stateless operators
 */
public class KStreamStateless {
    private static final String WORD_COUNT_INPUT_TOPIC_NAME = "week2-wordcount-input";

    public static void main(final String[] args) throws Exception {
        System.out.println("============== KStreamStateless.main ============= ");
        System.out.println("reading lines from:  " + WORD_COUNT_INPUT_TOPIC_NAME);
        System.out.println("============== KStreamStateless.main ============= ");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kstreams-stateless-app");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "kstreams-stateless-app-client");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_LIST);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

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
        KStream<String, String> lineStream = builder.stream(WORD_COUNT_INPUT_TOPIC_NAME);

        KStream<String, String> exceptionStream = lineStream.filter((key,line) ->
                line.contains("Exception"));

        // transform only the value to lowercase
        KStream<String, String> lowerCaseStream = exceptionStream.mapValues(line ->
                line.toLowerCase());

        lowerCaseStream.peek((key,value) ->
                System.out.printf("key: %s, value: %s\n", key,value));

        KStream<String, String> regularStream = lineStream.filterNot((key,line) ->
                line.contains("Exception"));
        regularStream.print(Printed.toSysOut());
        regularStream.print(Printed.toFile("output.out"));

        Predicate<String,String> sqlException = (key,line) -> line.contains("SQLException");
        Predicate<String,String> ioException = (key, line) -> line.contains("IOException");

        KStream<String, String>[] branches = lineStream.branch(sqlException, ioException);

        KStream<String, String> sqlExpStream = branches[0];
        KStream<String, String> ioExpStream = branches[1];

        System.out.println("Building topology");
        Topology topology = builder.build();
        System.out.println("topology: "  + topology.describe().toString());
        KafkaStreams streams = new KafkaStreams(topology, props);


        System.out.println("Start running the topology");
        streams.start();

        // shutdown hook to properly and gracefully close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}

package streamingcourse.week2.leaderboard;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import spark.Spark;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.StringJoiner;

/**
 * A simple web server to display top N users via browser.
 * It uses Java Spark as an embedded HTTP server.
 * It queries the state store using KafkaStreams
 *
 * See this resource for more details - https://docs.confluent.io/platform/current/streams/developer-guide/interactive-queries.html
 */
public class StoreQueryServer {
    private static Logger logger = LogManager.getLogger(StoreQueryServer.class.getName());
    private final String NO_RESULTS = "No Results Found";
    private final String APPLICATION_NOT_ACTIVE = "Application is not active. Try later.";

    private int port;
    private String stateStoreName;
    private KafkaStreams streams;
    private Boolean isActive = false;

    public StoreQueryServer(KafkaStreams streams, String stateStoreName, int port) {
        this.streams = streams;
        this.stateStoreName = stateStoreName;
        this.port = port;
    }

    void setActive(Boolean state) {
        isActive = state;
    }

    public void start() {
        logger.info("Starting Query Server at http://localhost:" + port + "/topN");

        Spark.port(port);
        Spark.get("/", (req, res) -> {
            Collection<StreamsMetadata> streamsMetadataList = streams.allMetadataForStore(stateStoreName);
            logger.info("found " + streamsMetadataList.size() + " stream metadata");
            StringJoiner joiner = new StringJoiner("", "<table width=\"500\">","</table>");
            streamsMetadataList.stream().forEach(streamsMetadata -> {
                joiner.add("<tr>");
                joiner.add("<td>").add(streamsMetadata.stateStoreNames().toString()).add("</td>");
                joiner.add("</tr>");
            });
            return joiner.toString();
        });

        Spark.get("/topN", (req, res) -> {
            TopNUsers topNUsers;
            String results;

            if (!isActive) {
                results = APPLICATION_NOT_ACTIVE;
            } else {
                topNUsers = getTopNResult();
                if (topNUsers == null) {
                    results = NO_RESULTS;
                } else {
                    List<UserTotalUsage> totalUsageList  = topNUsers.getTopNUserList();
                    StringJoiner joiner = new StringJoiner("", "<table width=\"500\">","</table>");
                    joiner.add("<tr><th>User</th><th>Used Bytes</th><th>Department</th></tr>");
                    totalUsageList.stream().forEach(userTotalUsage -> {
                        joiner.add("<tr>");
                        joiner.add("<td>").add(userTotalUsage.userName).add("</td>");
                        joiner.add("<td>").add(String.valueOf(userTotalUsage.totalBytesUsed)).add("</td>");
                        joiner.add("<td>").add(String.valueOf(userTotalUsage.deptId)).add("</td>");
                        joiner.add("</tr>");
                    });
                    results = joiner.toString();
                }
            }
            return results;
        });
    }

    private TopNUsers getTopNResult() {

        List<KeyValue<String, String>> localResults = new ArrayList<>();
        final QueryableStoreType<ReadOnlyKeyValueStore<String, TopNUsers>> queryableStoreType = QueryableStoreTypes.keyValueStore();
        StoreQueryParameters<ReadOnlyKeyValueStore<String, TopNUsers>> storeQueryParameters = StoreQueryParameters.fromNameAndType(stateStoreName, queryableStoreType);
        ReadOnlyKeyValueStore<String, TopNUsers> stateStore = streams.store(storeQueryParameters);

        TopNUsers value = stateStore.get("topNUser");
        return value;
    }

    void stop() {
        Spark.stop();
    }

    public static void main(String[] args) {

    }

}

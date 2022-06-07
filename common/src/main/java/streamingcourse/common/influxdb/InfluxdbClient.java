package streamingcourse.common.influxdb;


import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.*;
import java.util.stream.IntStream;

import com.github.javafaker.Faker;
import com.influxdb.annotations.Column;
import com.influxdb.annotations.Measurement;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApi;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class InfluxdbClient {
    private static Logger logger = LogManager.getLogger(InfluxdbClient.class);

    public static void main(final String[] args) {
        System.out.println("InfluxdbClient.main");

        // You can generate an API token from the "API Tokens Tab" in the UI
        String token = "y3R9cibtG5fyu4qpL0L_0b8AUB8hmITQRoDGUfba4HeodJeKwEJckkdlHf9tqJqPN80DeIGFc5a-3nJG9OjXZA==";
        String bucket = "top-level";
        String org = "streaming";
        String host = "http://localhost:8086";

        logger.info("=========== " + InfluxdbClient.class.getName() + " ===========");
        logger.info("bucket: " + bucket);
        logger.info("org: " + org);
        logger.info("host: " + host);
        logger.info(" ===================================================");

        InfluxDBClient client = InfluxDBClientFactory.create(host, token.toCharArray());

        WriteApiBlocking writeApi = client.getWriteApiBlocking();
        Faker faker = new Faker(new Random());

        Random userRandom = new Random();
        Random bytesUsedRandom = new Random();
        Long bytesUsed = 200L;
        List<String> userList = Arrays.asList("faker", "bobby","whoami", "justme", "walker", "lanes");

        Map<String, Long> userByteUsed = new HashMap<>();
        for (String user : userList) {
            userByteUsed.put(user, bytesUsed);
        }
        Instant startClock = Instant.now().minus(5, ChronoUnit.MINUTES);

        for (int idx = 1; idx < 500; idx++ ) {

            UserMobileUsage userMobileUsage = new UserMobileUsage();
            userMobileUsage.userName = userList.get(userRandom.nextInt(userList.size()));
            bytesUsed = userByteUsed.get(userMobileUsage.userName) + bytesUsedRandom.nextInt(233) + 30L;
            userMobileUsage.bytesUsed = bytesUsed;
            userByteUsed.put(userMobileUsage.userName, bytesUsed);
            Instant nextClock = startClock.plus(userRandom.nextInt(30), ChronoUnit.SECONDS);

            userMobileUsage.time = nextClock;
            startClock = nextClock;

            logger.info("====== writing: " + idx +  ": " + userMobileUsage.toString());
            writeApi.writeMeasurement(bucket, org, WritePrecision.MS, userMobileUsage);
            try {
                Thread.sleep(2000);
            } catch (Exception e) {

            }
        }

        client.close();
        logger.info("====== done writing ======");

    }

    @Measurement(name = "mobileusage")
    public static class UserMobileUsage {
        @Column(tag = true)
        String userName;
        @Column
        Long bytesUsed;
        @Column(timestamp = true)
        Instant time;

        @Override
        public String toString() {
            return "UserMobileUsage{" +
                    "userName='" + userName + '\'' +
                    ", bytesUsed=" + bytesUsed +
                    ", time=" + time +
                    '}';
        }
    }
}


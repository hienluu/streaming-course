package streamingcourse.common.influxdb;


import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
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
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class InfluxdbClient {
    private static Logger logger = LogManager.getLogger(InfluxdbClient.class);

    private static String org;
    private String bucket;
    private String host;
    private String token;

    private InfluxDBClient client;
    private WriteApiBlocking writeApi;

    /**
     * Using default bucket
     */
    public InfluxdbClient() {
        init();
    }
    public InfluxdbClient(String bucket) {
        init();
        this.bucket = bucket;
    }

    public void init() {

        try (InputStream input = new FileInputStream("common/src/main/resources/influxdb.properties")) {

            Properties prop = new Properties();

            // load a properties file
            prop.load(input);

            host = prop.getProperty("host");
            org = prop.getProperty("org");
            token = prop.getProperty("token");
            bucket = prop.getProperty("bucket");

        } catch (Exception e) {
            throw new RuntimeException("Encountered error while reading influxdb.properties", e);
        }


        client = InfluxDBClientFactory.create(host, token.toCharArray());
        writeApi = client.getWriteApiBlocking();
    }

    public void printProperties() {
        logger.info(" ========== InfluxDB properties =====================");
        logger.info("host: " + host);
        logger.info("org: " + org);
        logger.info("token: " + token);
        logger.info("bucket: " + bucket);
        logger.info(" ===================================================");
    }

    public void writeMeasurement(Object measurement) {
        writeApi.writeMeasurement(bucket, org, WritePrecision.MS, measurement);
    }

    public void close() {
        logger.info("closing InfluxDBClient");
        client.close();
    }


    public static void main(final String[] args) {
        System.out.println("InfluxdbClient.main");

        InfluxdbClient client = new InfluxdbClient();
        client.printProperties();


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
            client.writeMeasurement(userMobileUsage);
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


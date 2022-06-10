package streamingcourse.common.influxdb;


import java.io.FileInputStream;
import java.io.InputStream;
import java.time.Instant;
import java.util.*;

import com.influxdb.annotations.Column;
import com.influxdb.annotations.Measurement;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.OrganizationsQuery;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.Bucket;
import com.influxdb.client.domain.Organization;
import com.influxdb.client.domain.WritePrecision;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class InfluxdbClient {
    private static Logger logger = LogManager.getLogger(InfluxdbClient.class);

    private static String orgName;
    private String bucketName;
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
        this.bucketName = bucket;
        init();
    }

    public void init() {

        try (InputStream input = new FileInputStream("common/src/main/resources/influxdb.properties")) {

            Properties prop = new Properties();

            // load a properties file
            prop.load(input);

            host = prop.getProperty("host");
            orgName = prop.getProperty("org");
            token = prop.getProperty("token");
            if (bucketName == null) {
                bucketName = prop.getProperty("bucket");
            } else {
                logger.info("Not using default bucket name. Using the provided name: " + bucketName);
            }

        } catch (Exception e) {
            throw new RuntimeException("Encountered error while reading influxdb.properties", e);
        }

        try {
            client = InfluxDBClientFactory.create(host, token.toCharArray());

            Bucket bucket = client.getBucketsApi().findBucketByName(bucketName);
            if (bucket == null) {
                logger.info("Unable to find bucket: "+ bucketName + ".  Will create a new one with org: " + orgName);
                OrganizationsQuery organizationsQuery = new OrganizationsQuery();
                organizationsQuery.setOrg(orgName);

                List<Organization> organizationList = client.getOrganizationsApi().findOrganizations(organizationsQuery);
                if (organizationList != null) {
                    Organization topOrg = organizationList.get(0);
                    logger.info("org: " + topOrg);
                    client.getBucketsApi().createBucket(bucketName, topOrg.getId());
                    logger.info("Successfully created bucket: " + bucketName);
                } else {
                    throw  new RuntimeException("Unable to find org. with name: " + orgName);
                }
            }

            writeApi = client.getWriteApiBlocking();
        } catch (Exception e) {
            throw new RuntimeException("Encountered error while initializing  InfluxDBClient", e);
        }

    }

    public void printProperties() {
        logger.info(" ========== InfluxDB properties =====================");
        logger.info("host: " + host);
        logger.info("org: " + orgName);
        logger.info("token: " + token);
        logger.info("bucket: " + bucketName);
        logger.info(" ===================================================");
    }

    public void writeMeasurement(Object measurement) {
        writeApi.writeMeasurement(bucketName, orgName, WritePrecision.MS, measurement);
    }

    public void close() {
        logger.info("closing InfluxDBClient");
        client.close();
    }


    public static void main(final String[] args) {
        System.out.println("InfluxdbClient.main");

        InfluxdbClient client = new InfluxdbClient("test-bucket");
        client.printProperties();

        /*

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


         */
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


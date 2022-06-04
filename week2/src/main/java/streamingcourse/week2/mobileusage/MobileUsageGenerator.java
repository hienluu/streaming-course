package streamingcourse.week2.mobileusage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.github.javafaker.Faker;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.IntStream;

public class MobileUsageGenerator {
    private  List<String> userNameList;
    private  Map<String, String> deptMap;
    private Faker faker = new Faker();
    private Random userIdRandomGen = new Random();
    private Random deptRandomGen = new Random();
    private Random byteUsedRandomGen = new Random();
    private Random timeRandomGen = new Random();

    private Instant clock;


    public MobileUsageGenerator(int numDaysInThePast) {
        if (numDaysInThePast < 1) {
            throw  new IllegalArgumentException("numDaysInThePast can't be less than 1");
        }

        userNameList = Arrays.asList("jb", "mjane", "skunky",  "bobby", "dreamer",
                "onetimer", "skywalker", "theman",  "flyer", "hiker",
                "theone", "gamer", "justdoit",  "whoami", "youareit");

        List<String> deptList = Arrays.asList("it","engr","finance", "hr");

        deptMap = new HashMap<>(userNameList.size());
        // popular the deptMap
        IntStream.range(0, userNameList.size()).forEach(idx -> {
            deptMap.put(userNameList.get(idx), deptList.get(idx % deptList.size()));
        });



        clock = Instant.now().minus(numDaysInThePast, ChronoUnit.DAYS);
    }

    public MobileUsage next() {
        MobileUsage mobileUsage = new MobileUsage();

        int randomUserIdx = userIdRandomGen.nextInt(userNameList.size());
        mobileUsage.userName = userNameList.get(randomUserIdx);
        mobileUsage.dept = deptMap.get(mobileUsage.userName);
        mobileUsage.bytesUsed = byteUsedRandomGen.nextInt(1000) + 35;

        // timeStamp is advancing from the clock at the minimum of 30 seconds
        Instant timeStamp = clock.plus(timeRandomGen.nextInt(30) + 15, ChronoUnit.SECONDS);
        mobileUsage.timeStamp = timeStamp;

        // update the clock so it is advancing.  This means the clock will advance between records
        clock = timeStamp;

        return mobileUsage;
    }

    public static void main(String[] args) throws Exception {
        MobileUsageGenerator gen = new MobileUsageGenerator(5);
        ObjectMapper objectMapper =
                JsonMapper.builder()
                        .build().findAndRegisterModules();

        for (int i = 1; i < 10; i++) {
            MobileUsage mobileUsage = gen.next();
            System.out.println(i + ": " + mobileUsage);
            String valueInJson = objectMapper.writeValueAsString(mobileUsage);
            System.out.println(i + ": " + valueInJson);
        }
    }
}

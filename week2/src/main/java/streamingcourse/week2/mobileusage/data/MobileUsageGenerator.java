package streamingcourse.week2.mobileusage.data;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.github.javafaker.Faker;
import streamingcourse.week2.mobileusage.model.DeptInfo;
import streamingcourse.week2.mobileusage.model.MobileUsage;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.IntStream;

/**
 * Generate random mobile usage data - leveraging Java Faker
 * - https://github.com/DiUS/java-faker
 */
public class MobileUsageGenerator {
    private  List<String> userNameList;
    private  Map<String, DeptInfo> userNameToDeptMap;
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

        List<DeptInfo> deptInfoList =  DeptGenerator.getInstance().getDeptInfoList();

        List<Integer> deptIdList = Arrays.asList(101, 105, 201, 205, 301, 305);

        userNameToDeptMap = new HashMap<>(userNameList.size());
        // popular the deptIdMap
        IntStream.range(0, userNameList.size()).forEach(idx -> {
            String userName = userNameList.get(idx);
            DeptInfo deptInfo = deptInfoList.get(idx % deptIdList.size());
            userNameToDeptMap.put(userName, deptInfo);
        });

        clock = Instant.now().minus(numDaysInThePast, ChronoUnit.DAYS);
    }

    public MobileUsage next() {
        MobileUsage mobileUsage = new MobileUsage();

        int randomUserIdx = userIdRandomGen.nextInt(userNameList.size());
        mobileUsage.userName = userNameList.get(randomUserIdx);
        mobileUsage.deptId = userNameToDeptMap.get(mobileUsage.userName).getDeptIt();
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

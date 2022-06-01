package streamingcourse.week2.mobileusage;

import com.github.javafaker.Faker;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class MobileUsageGenerator {
    private  List<String> userNameList;
    private Faker faker = new Faker();
    private Random userIdRandomGen = new Random();
    private Random byteUsedRandomGen = new Random();
    private Random timeRandomGen = new Random();

    private Instant clock;


    public MobileUsageGenerator(int numDaysInThePast) {
        if (numDaysInThePast < 1) {
            throw  new IllegalArgumentException("numDaysInThePast can't be less than 1");
        }
        userNameList = Arrays.asList("jb", "mjane", "skunky", "tweetie", "bobby", "dreamer",
                "onetimer", "skywalker", "theman", "walker", "flyer", "hiker",
                "theone", "gamer", "justdoit", "drinker", "whoami", "youareit");

        clock = Instant.now().minus(numDaysInThePast, ChronoUnit.DAYS);
    }

    public MobileUsage next() {
        MobileUsage mobileUsage = new MobileUsage();
        int randomUserIdx = userIdRandomGen.nextInt(userNameList.size());
        mobileUsage.userName = userNameList.get(randomUserIdx);
        mobileUsage.bytesUsed = byteUsedRandomGen.nextInt(1000) + 35;

        // timeStamp is advancing from the clock at the minimum of 30 seconds
        Instant timeStamp = clock.plus(timeRandomGen.nextInt(30) + 15, ChronoUnit.SECONDS);
        mobileUsage.timeStamp = timeStamp;

        // update the clock so it is advancing.  This means the clock will advance between records
        clock = timeStamp;

        return mobileUsage;
    }

    public static void main(String[] args) {
        MobileUsageGenerator gen = new MobileUsageGenerator(5);

        for (int i = 1; i < 50; i++) {
            System.out.println(i + ": " + gen.next());
        }
    }
}

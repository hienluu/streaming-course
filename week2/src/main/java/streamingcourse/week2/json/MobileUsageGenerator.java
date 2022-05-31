package streamingcourse.week2.json;

import com.github.javafaker.Faker;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.*;

public class MobileUsageGenerator {
    private  List<String> userNameList;
    private Faker faker = new Faker();
    private Random userIdRandomGen = new Random();
    private Random byteUsedRandomGen = new Random();
    private Random timeRandomGen = new Random();

    private Instant now = Instant.now();

    private Map<String,Instant> timerPerUser;
    public MobileUsageGenerator(int numDaysInThePast) {
        if (numDaysInThePast < 1) {
            throw  new IllegalArgumentException("numDaysInThePast can't be less than 1");
        }
        userNameList = Arrays.asList("jb", "mjane", "skunky", "tweetie", "bobby", "dreamer",
                "onetimer", "skywalker", "theman", "walker", "flyer", "hiker",
                "theone", "hithere", "justdoit", "whynot", "whoami", "youareit",
                "gamer", "drinker");

        timerPerUser = new HashMap<>(userNameList.size());
        userNameList.stream().forEach(userName -> {
            timerPerUser.put(userName, Instant.now().minus(numDaysInThePast, ChronoUnit.DAYS));
        });

    }

    public MobileUsage next() {
        MobileUsage mobileUsage = new MobileUsage();
        int randomUserIdx = userIdRandomGen.nextInt(userNameList.size());
        mobileUsage.userName = userNameList.get(randomUserIdx);
        mobileUsage.bytesUsed = byteUsedRandomGen.nextInt(1000) + 35;

        Instant timerForUser = timerPerUser.get(mobileUsage.userName);

        Instant newTimerForUser = timerForUser.plus(timeRandomGen.nextInt(120) + 30, ChronoUnit.SECONDS);
        // update the map w/ the updated timer
        timerPerUser.put(mobileUsage.userName, newTimerForUser);

        mobileUsage.timeStamp = newTimerForUser;

        return mobileUsage;
    }

    public static void main(String[] args) {
        MobileUsageGenerator gen = new MobileUsageGenerator(5);

        for (int i = 1; i < 50; i++) {
            System.out.println(i + ": " + gen.next());
        }
    }
}

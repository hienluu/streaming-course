package streamingcourse.week2.leaderboard;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.TreeSet;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "topNMobileUsage"
})
public class TopNUsers {
    private static Logger log = LogManager.getLogger(TopNUsers.class.getName());
    private ObjectMapper mapper =
            JsonMapper.builder()
                    .build().findAndRegisterModules();
    private int topN = 3;
    public TopNUsers() {
    }
    public TopNUsers(int n) {
        if (n < 1) {
            throw new IllegalArgumentException("n can't be less than 1");
        }
        this.topN = n;
    }
    private final TreeSet<UserTotalUsage> top3MobileUsageSorted = new TreeSet<>((o1, o2) -> {
        // tree uses the comparator to determine if two objects are the same
        // when two UserTotalUsage instances for the same user, then we want to return 0
        // to indicate they are the same
        if (Objects.equals(o1, o2)) {
            return 0;
        }
        final long result = o2.getTotalBytesUsed() - o1.getTotalBytesUsed();
        if (result != 0)
            return (int)result;
        else
            return o1.getUserName().compareTo(o2.getUserName());
    });

    public TopNUsers add(UserTotalUsage newUserTotalUsage) {
        top3MobileUsageSorted.add(newUserTotalUsage);
        if (top3MobileUsageSorted.size() > topN) {
            top3MobileUsageSorted.remove(top3MobileUsageSorted.last());
        }
        return  this;
    }

    public TopNUsers remove(UserTotalUsage oldUserTotalUsage) {
        boolean contains = top3MobileUsageSorted.remove(oldUserTotalUsage);
        return this;
    }

    public List<UserTotalUsage> getTopNUserList() {
        List<UserTotalUsage> result = new ArrayList<>(topN);
        top3MobileUsageSorted.stream().forEach( userTotalUsage -> result.add(userTotalUsage));
        return result;
    }

    @JsonProperty("topNMobileUsage")
    public String getTop3Sorted()  {
        try {
            return mapper.writeValueAsString(top3MobileUsageSorted);
        } catch (Exception e) {
            throw  new SerializationException(e);
        }
    }

    @JsonProperty("topNMobileUsage")
    public void setTop3Sorted(String top3String) throws IOException {
        try {
            UserTotalUsage[] top3 = mapper.readValue(top3String, UserTotalUsage[].class);
            for (UserTotalUsage i : top3) {
                add(i);
            }
        } catch (Exception e) {
            throw  new SerializationException(e);
        }
    }

    public static void main(String[] args) throws Exception {
        UserTotalUsage userTotalUsage1 = new UserTotalUsage("jb", 1460, 101);
        UserTotalUsage userTotalUsage2 = new UserTotalUsage("whoami", 1410, 201);
        UserTotalUsage userTotalUsage3 = new UserTotalUsage("hiker", 1318, 301);

        TopNUsers topNUsers = new TopNUsers();
        topNUsers.add(userTotalUsage1).add(userTotalUsage2).add(userTotalUsage3);

        UserTotalUsage userTotalUsage4 = new UserTotalUsage("jb", 5000, 101);
        System.out.println("remove: " + topNUsers.remove(userTotalUsage4));

        System.out.println("equals: " + userTotalUsage4.equals(userTotalUsage1));
    }

}

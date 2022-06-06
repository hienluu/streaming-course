package streamingcourse.week2.leaderboard;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;

public class UserTotalUsage {
    private static Logger log = LogManager.getLogger(UserTotalUsage.class.getName());
    public String userName;
    public long totalBytesUsed;
    public int deptId;

    public UserTotalUsage() {}

    public UserTotalUsage(String userName, long totalBytesUsed, int deptId) {
        this.userName = userName;
        this.totalBytesUsed = totalBytesUsed;
        this.deptId = deptId;
    }
    public UserTotalUsage increaseBy(long bytesUsed) {
        if (bytesUsed < 0) {
            throw  new IllegalArgumentException("bytesUsed can't be less than 0");
        }
        totalBytesUsed += bytesUsed;
        return  this;
    }

    public String getUserName() {
        return userName;
    }

    public long getTotalBytesUsed() {
        return totalBytesUsed;
    }

    public int getDeptId() {
        return deptId;
    }

    @Override
    public String toString() {
        return "UserTotalUsage{" +
                "userName='" + userName + '\'' +
                ", totalBytesUsed=" + totalBytesUsed +
                ", deptId=" + deptId +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UserTotalUsage that = (UserTotalUsage) o;
        boolean isSame = Objects.equals(userName, that.userName);
        return  isSame;
    }

    @Override
    public int hashCode() {
        return Objects.hash(userName);
    }
}

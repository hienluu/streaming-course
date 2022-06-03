package streamingcourse.week2.mobileusage;

import java.time.Instant;

public class TotalMobileUsage implements Comparable<TotalMobileUsage> {
    private String userName;
    private long totalUsage;
    private Instant timeStamp;

    public TotalMobileUsage(MobileUsage mobileUsage) {
        this.userName = mobileUsage.userName;
        this.totalUsage = mobileUsage.bytesUsed;
        this.timeStamp = mobileUsage.timeStamp;
    }

    @Override
    public int compareTo(TotalMobileUsage o) {
        return Long.compare(totalUsage, o.totalUsage);
    }

    public String getUserName() {
        return userName;
    }

    public long getTotalUsage() {
        return totalUsage;
    }

    public Instant getTimeStamp() {
        return timeStamp;
    }

    @Override
    public String toString() {
        return "TotalMobileUsage [" +
                "userName='" + userName + '\'' +
                ", totalUsage=" + totalUsage +
                ", timeStamp=" + timeStamp +
                ']';
    }
}

package streamingcourse.week2.mobileusage;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

public class TopMobileUsage {
    private final TreeSet<TotalMobileUsage> mobileUsageLeaderBoard = new TreeSet<>();

    public TopMobileUsage add(final TotalMobileUsage totalMobileUsage) {
        mobileUsageLeaderBoard.add(totalMobileUsage);

        // keep only the top 3 total mobile usage
        if (mobileUsageLeaderBoard.size() > 3) {
            mobileUsageLeaderBoard.remove(mobileUsageLeaderBoard.last());
        }

        return this;
    }

    public List<TotalMobileUsage> toList() {

        Iterator<TotalMobileUsage> scores = mobileUsageLeaderBoard.iterator();
        List<TotalMobileUsage> leaderBoardCopy = new ArrayList<>();
        while (scores.hasNext()) {
            leaderBoardCopy.add(scores.next());
        }

        return leaderBoardCopy;
    }
}

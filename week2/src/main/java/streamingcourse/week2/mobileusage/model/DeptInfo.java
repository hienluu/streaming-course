package streamingcourse.week2.mobileusage.model;

import java.util.Objects;

public class DeptInfo {
    public int deptIt;
    public String name;
    public long quota;
    public String leader;

    public int getDeptIt() {
        return deptIt;
    }

    public String getName() {
        return name;
    }

    public long getQuota() {
        return quota;
    }

    public String getLeader() {
        return leader;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DeptInfo deptInfo = (DeptInfo) o;
        return deptIt == deptInfo.deptIt && quota == deptInfo.quota && name.equals(deptInfo.name) && leader.equals(deptInfo.leader);
    }

    @Override
    public int hashCode() {
        return Objects.hash(deptIt, name, quota, leader);
    }

    @Override
    public String toString() {
        return "DeptInfo{" +
                "deptIt=" + deptIt +
                ", name='" + name + '\'' +
                ", quota=" + quota +
                ", leader='" + leader + '\'' +
                '}';
    }
}

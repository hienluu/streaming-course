package streamingcourse.week2.mobileusage.data;

import streamingcourse.week2.common.MyJsonSerializer;
import streamingcourse.week2.mobileusage.model.DeptInfo;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class DepartmentInfoGenerator {
    private static DepartmentInfoGenerator instance;
    private List<DeptInfo> deptInfoList;
    private DepartmentInfoGenerator() {
        deptInfoList = new ArrayList<>();
        readFromFile();
    }

    private void readFromFile() {
        try {
            Scanner scanner = new Scanner(
                    new File("week2/src/main/resources/dept-list.txt"));

            MyJsonSerializer myJsonSerializer = MyJsonSerializer.build();
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                DeptInfo deptInfo = (DeptInfo)myJsonSerializer.fromJson(line, DeptInfo.class);

                deptInfoList.add(deptInfo);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public List<DeptInfo> getDeptInfoList() {
        return deptInfoList;
    }

    public static DepartmentInfoGenerator getInstance() {
        if (instance == null) {
            // yes, not thread safe
            instance = new DepartmentInfoGenerator();
        }
        return instance;
    }

    public static  void  main(String[] args) {
        System.out.println("======= DeptGenerator.main ========");
        List<DeptInfo> deptInfos =  DepartmentInfoGenerator.getInstance().getDeptInfoList();
        deptInfos.stream().forEach(deptInfo -> {
            System.out.println(deptInfo);
        });
    }
}

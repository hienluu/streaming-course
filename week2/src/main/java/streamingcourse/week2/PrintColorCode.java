package streamingcourse.week2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PrintColorCode {
    private static final String ANSI_RESET = "\u001B[0m";
    private static final String ANSI_GREEN = "\u001B[32m";
    private static final String ANSI_PURPLE = "\u001B[35m";
    private static final String ANSI_BLUE = "\u001B[34m";

    private static final String ANSI_YELLOW = "\u001B[33m";

    public static String ansiGreen() {
        return ANSI_GREEN;
    }

    public static String ansiBlue() {
        return ANSI_BLUE;
    }

    public static String ansiPurple() {
        return ANSI_PURPLE;
    }

    public static String ansiYellow() {
        return ANSI_YELLOW;
    }

    public static String ansiReset() {
        return ANSI_RESET;
    }

    private static List<String> ansiColorList = new ArrayList<>();
    static {
        ansiColorList = Arrays.asList(ANSI_BLUE, ANSI_GREEN, ANSI_PURPLE, ANSI_YELLOW);
    }

    public static String getAnsiColor(int idx) {
        if (idx < 0) {
            return ansiColorList.get(0);
        } else if (idx > ansiColorList.size()) {
            // the last one
            return ansiColorList.get(ansiColorList.size()-1);
        } else {
            return ansiColorList.get(idx);
        }
    }
}


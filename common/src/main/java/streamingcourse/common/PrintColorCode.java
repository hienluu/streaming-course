package streamingcourse.common;

public class PrintColorCode {
    private static final String ANSI_RESET = "\u001B[0m";
    private static final String ANSI_GREEN = "\u001B[32m";
    private static final String ANSI_PURPLE = "\u001B[35m";
    private static final String ANSI_BLUE = "\u001B[34m";

    private static final String YELLOW_BLUE = "\u001B[33m";

    public static String getAnsiGreen() {
        return ANSI_GREEN;
    }

    public static String getAnsiBlue() {
        return ANSI_BLUE;
    }

    public static String getAnsiPurple() {
        return ANSI_PURPLE;
    }

    public static String getAnsiYellow() {
        return YELLOW_BLUE;
    }

    public static String getAnsiReset() {
        return ANSI_RESET;
    }

}

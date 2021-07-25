package it.unitn.ds1.common;

public class Log {

    private static LogLevel setLevel = null;

    public static void initializeLog(LogLevel level) {
        setLevel = level;
    }

    public static void partialLog(boolean startOfLine, LogLevel level, int id, String s) {
        if (level == null) {
            throw new RuntimeException("Logging must have a level!");
        }

        if (setLevel == null) {
            throw new RuntimeException("Initialize Log class first.");
        }

        if (setLevel.compareTo(level) <= 0) {
            if (startOfLine) {
                System.out.format("[%s] %2d: %s", level, id, s);
            } else {
                System.out.format("%s", s);
            }
        }
    }

    public static void log(LogLevel level, int id, String s) {
        if (level == null) {
            throw new RuntimeException("Logging must have a level!");
        }

        if (setLevel == null) {
            throw new RuntimeException("Initialize Log class first.");
        }

        if (setLevel.compareTo(level) <= 0) {
            System.out.format("[%s] %2d: %s\n", level, id, s);
        }
    }
}

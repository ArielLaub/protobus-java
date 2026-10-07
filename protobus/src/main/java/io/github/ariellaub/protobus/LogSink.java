package io.github.ariellaub.protobus;

/**
 * Where protobus writes its log lines. Install one with {@link Logger#set}.
 *
 * Level filtering happens before a sink is called, so a sink never sees a line
 * below {@link Logger#getLevel()}. A sink may be called from any thread.
 */
public interface LogSink {
    void debug(String message);

    void info(String message);

    void warn(String message);

    void error(String message);

    /**
     * A sink that also takes structured records. {@link Log} hands one the
     * {@link LogRecord} itself rather than its rendering.
     */
    interface Structured extends LogSink {
        void log(LogRecord record);
    }
}

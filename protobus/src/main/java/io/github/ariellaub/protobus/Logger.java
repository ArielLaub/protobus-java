package io.github.ariellaub.protobus;

import java.net.URI;
import java.time.Instant;

/**
 * The library's logger: a level threshold in front of a replaceable sink.
 *
 * The default sink is SLF4J (logger {@code protobus}) when an SLF4J provider is
 * on the class path, and the console otherwise. The threshold starts from
 * {@code LOG_LEVEL} (debug, info, warn, error, silent; default info).
 *
 * protobus never logs a message body: request, reply and event payloads routinely
 * carry credentials and personal data, so log lines name types, sizes and ids.
 */
public final class Logger {
    private Logger() {}

    private static volatile LogLevel level = LogLevel.fromEnv();
    private static volatile LogSink sink = defaultSink();

    public static void setLevel(LogLevel newLevel) {
        level = newLevel;
    }

    public static LogLevel getLevel() {
        return level;
    }

    /** Replace the sink. Null restores the default. */
    public static void set(LogSink newSink) {
        sink = newSink == null ? defaultSink() : newSink;
    }

    static LogSink sink() {
        return sink;
    }

    static boolean enabled(LogLevel at) {
        return level.rank <= at.rank;
    }

    public static void debug(String message) {
        if (enabled(LogLevel.DEBUG)) sink.debug(message);
    }

    public static void info(String message) {
        if (enabled(LogLevel.INFO)) sink.info(message);
    }

    public static void warn(String message) {
        if (enabled(LogLevel.WARN)) sink.warn(message);
    }

    public static void error(String message) {
        if (enabled(LogLevel.ERROR)) sink.error(message);
    }

    /**
     * Strip the password out of a broker URL so it is safe to log. The user, host,
     * port and vhost are kept, since they are what makes the line useful. Anything
     * that does not parse is reported as {@code <redacted>}: it may still be a
     * credential.
     */
    public static String redactUrl(String url) {
        if (url == null || url.isEmpty()) return String.valueOf(url);
        try {
            URI uri = new URI(url);
            if (uri.getScheme() == null || uri.getRawAuthority() == null) return "<redacted>";
            String userInfo = uri.getRawUserInfo();
            if (userInfo == null || !userInfo.contains(":")) return url;
            String user = userInfo.substring(0, userInfo.indexOf(':'));
            String authority = uri.getRawAuthority();
            String hostPart = authority.substring(authority.indexOf('@') + 1);
            StringBuilder out = new StringBuilder(uri.getScheme()).append("://").append(user).append(":***@")
                    .append(hostPart);
            if (uri.getRawPath() != null) out.append(uri.getRawPath());
            if (uri.getRawQuery() != null) out.append('?').append(uri.getRawQuery());
            return out.toString();
        } catch (Exception e) {
            return "<redacted>";
        }
    }

    private static LogSink defaultSink() {
        try {
            Class.forName("org.slf4j.LoggerFactory");
            LogSink slf4j = Slf4jSink.createIfBound();
            if (slf4j != null) return slf4j;
        } catch (ClassNotFoundException | LinkageError ignored) {
            // No SLF4J at all: the console it is.
        }
        return new ConsoleSink();
    }

    /** Writes info and debug to stdout, warnings and errors to stderr. */
    public static final class ConsoleSink implements LogSink {
        @Override
        public void debug(String message) {
            System.out.println(stamp("DEBUG", message));
        }

        @Override
        public void info(String message) {
            System.out.println(stamp("INFO", message));
        }

        @Override
        public void warn(String message) {
            System.err.println(stamp("WARN", message));
        }

        @Override
        public void error(String message) {
            System.err.println(stamp("ERROR", message));
        }

        private static String stamp(String lvl, String message) {
            return Instant.now() + " " + lvl + " protobus: " + message;
        }
    }

    static final class Slf4jSink implements LogSink {
        private final org.slf4j.Logger log;

        private Slf4jSink(org.slf4j.Logger log) {
            this.log = log;
        }

        static LogSink createIfBound() {
            org.slf4j.ILoggerFactory factory = org.slf4j.LoggerFactory.getILoggerFactory();
            if (factory instanceof org.slf4j.helpers.NOPLoggerFactory) return null;
            return new Slf4jSink(factory.getLogger("protobus"));
        }

        @Override
        public void debug(String message) {
            log.debug(message);
        }

        @Override
        public void info(String message) {
            log.info(message);
        }

        @Override
        public void warn(String message) {
            log.warn(message);
        }

        @Override
        public void error(String message) {
            log.error(message);
        }
    }
}

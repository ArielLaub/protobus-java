package io.github.ariellaub.protobus;

import java.util.Locale;

/** Logging thresholds, from the most verbose. The initial level comes from {@code LOG_LEVEL}. */
public enum LogLevel {
    DEBUG(10), INFO(20), WARN(30), ERROR(40), SILENT(100);

    final int rank;

    LogLevel(int rank) {
        this.rank = rank;
    }

    static LogLevel fromEnv() {
        String raw = Config.raw("LOG_LEVEL");
        switch (raw == null ? "" : raw.trim().toLowerCase(Locale.ROOT)) {
            case "debug": return DEBUG;
            case "warn": case "warning": return WARN;
            case "error": return ERROR;
            case "silent": case "off": case "none": return SILENT;
            default: return INFO;
        }
    }
}

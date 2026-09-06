package in.gov.uidai.dp.velocity.engine.utils;

import lombok.extern.slf4j.Slf4j;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

@Slf4j
public final class TimeUtils {

    public static final ZoneId INDIA_ZONE = ZoneId.of("Asia/Kolkata");
    // Format: "YYYY-MM-DD HH:MM:SS" in IST — no milliseconds, space-separated.
    // This is consumed by both the Go backend (parseIST) and the React frontend (istUtils.js).
    // Avoid ISO-8601 'T' separator or milliseconds to prevent cross-browser Date parsing issues.
    public static final DateTimeFormatter IST_FMT = DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss")
            .withZone(INDIA_ZONE);

    private TimeUtils() {}

    public static long isoStringToEpochMs(String ts) {
        if (ts == null || ts.isBlank()) return -1L;
        try {
            return Instant.parse(ts).toEpochMilli();
        } catch (DateTimeParseException ignored) {}

        try {
            LocalDateTime ldt = LocalDateTime.parse(ts, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            return ldt.atZone(INDIA_ZONE).toInstant().toEpochMilli();
        } catch (DateTimeParseException e) {
            log.warn("Cannot parse timestamp '{}': {}", ts, e.getMessage());
            return -1L;
        }
    }

    public static String epochMsToIstString(long epochMs) {
        return IST_FMT.format(Instant.ofEpochMilli(epochMs));
    }

    public static String currentIstString() {
        return IST_FMT.format(Instant.now());
    }

    /**
     * Inverse of {@link #epochMsToIstString}/{@link #currentIstString}: parses a
     * naive IST "yyyy-MM-dd HH:mm:ss" string (e.g. an AggregationResult/
     * AnomalyEvent's producedAt) back to epoch millis. Returns -1 on failure —
     * callers doing delay-metric math must treat that as "timestamp unavailable"
     * and skip the observation, not feed -1 into a subtraction.
     */
    public static long istStringToEpochMs(String istString) {
        if (istString == null || istString.isBlank()) return -1L;
        try {
            LocalDateTime ldt = LocalDateTime.parse(istString, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            return ldt.atZone(INDIA_ZONE).toInstant().toEpochMilli();
        } catch (DateTimeParseException e) {
            log.warn("Cannot parse IST timestamp '{}': {}", istString, e.getMessage());
            return -1L;
        }
    }

    public static long floorToSlide(long epochMs, long slideMs, long offsetMs) {
        return Math.floorDiv(epochMs - offsetMs, slideMs) * slideMs + offsetMs;
    }

    public static long floorToSlide(long epochMs, long slideMs) {
        return floorToSlide(epochMs, slideMs, 0L);
    }

    public static String bucketKey(String alias, long bucketTs) {
        return alias + '#' + bucketTs;
    }

    public static String extractAlias(String bucketKey) {
        int idx = bucketKey.lastIndexOf('#');
        return bucketKey.substring(0, idx);
    }

    public static long extractBucketTs(String bucketKey) {
        int idx = bucketKey.lastIndexOf('#');
        return Long.parseLong(bucketKey.substring(idx + 1));
    }
}
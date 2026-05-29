package in.gov.uidai.dp.velocity.engine.utils;

import lombok.extern.slf4j.Slf4j;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * Timestamp utilities. All formatted strings are in IST (Asia/Kolkata) to
 * avoid confusion across DCs.
 */
@Slf4j
public final class TimeUtils {

    public static final ZoneId        INDIA_ZONE  = ZoneId.of("Asia/Kolkata");
    public static final DateTimeFormatter IST_FMT = DateTimeFormatter
            .ofPattern("yyyy-MM-dd'T'HH:mm:ss")
            .withZone(INDIA_ZONE);

    private TimeUtils() {}

    /**
     * Parse an ISO-8601 string (with or without offset/zone) to epoch milliseconds, IST.
     * Returns -1 and logs a warning on parse failure.
     */
    public static long isoStringToEpochMs(String ts) {
        if (ts == null || ts.isBlank()) return -1L;
        try {
            // Try with timezone / offset first
            return Instant.parse(ts).toEpochMilli();
        } catch (DateTimeParseException ignored) {}

        try {
            // Try as plain local datetime, treat as IST
            LocalDateTime ldt = LocalDateTime.parse(ts, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            return ldt.atZone(INDIA_ZONE).toInstant().toEpochMilli();
        } catch (DateTimeParseException e) {
            log.warn("Cannot parse timestamp '{}': {}", ts, e.getMessage());
            return -1L;
        }
    }

    /** Convert epoch milliseconds to IST-formatted string. */
    public static String epochMsToIstString(long epochMs) {
        return IST_FMT.format(Instant.ofEpochMilli(epochMs));
    }

    /** Current time as IST-formatted string. */
    public static String currentIstString() {
        return IST_FMT.format(Instant.now());
    }

    /**
     * Floor an epoch-ms timestamp to the nearest slide boundary.
     * E.g. for slide=60000ms, timestamps within the same minute map to the same bucket.
     */
    public static long floorToSlide(long epochMs, long slideMs) {
        return Math.floorDiv(epochMs, slideMs) * slideMs;
    }

    /** Compose a MapState key: {@code "alias#bucketTs"} */
    public static String bucketKey(String alias, long bucketTs) {
        return alias + '#' + bucketTs;
    }

    /** Extract alias from a bucket key produced by {@link #bucketKey}. */
    public static String extractAlias(String bucketKey) {
        int idx = bucketKey.lastIndexOf('#');
        return bucketKey.substring(0, idx);
    }

    /** Extract bucket timestamp from a bucket key produced by {@link #bucketKey}. */
    public static long extractBucketTs(String bucketKey) {
        int idx = bucketKey.lastIndexOf('#');
        return Long.parseLong(bucketKey.substring(idx + 1));
    }
}
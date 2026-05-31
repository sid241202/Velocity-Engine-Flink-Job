package in.gov.uidai.dp.velocity.engine.utils;

import lombok.extern.slf4j.Slf4j;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

@Slf4j
public final class TimeUtils {

    public static final ZoneId        INDIA_ZONE  = ZoneId.of("Asia/Kolkata");
    public static final DateTimeFormatter IST_FMT = DateTimeFormatter
            .ofPattern("yyyy-MM-dd'T'HH:mm:ss")
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

    public static long floorToSlide(long epochMs, long slideMs) {
        return Math.floorDiv(epochMs, slideMs) * slideMs;
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
package com.tizitec.parquet2csv;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;

/**
 * Logs row-level progress at a configurable interval during Parquet reading.
 *
 * <p>Prints a progress line every {@code intervalRows} rows, showing:
 * <ul>
 *   <li>Rows processed so far</li>
 *   <li>Elapsed time</li>
 *   <li>Throughput in rows/second</li>
 * </ul>
 *
 * <p>Call {@link #tick()} for every row, {@link #done()} when conversion completes.
 * Both are cheap — tick() only logs when the interval threshold is crossed.
 */
public class ProgressLogger {

    private static final Logger log = LoggerFactory.getLogger(ProgressLogger.class);

    private static final long DEFAULT_INTERVAL = 100_000L;

    private final String fileName;
    private final long intervalRows;
    private final Instant startTime;

    private long rowCount = 0;

    public ProgressLogger(String fileName) {
        this(fileName, DEFAULT_INTERVAL);
    }

    public ProgressLogger(String fileName, long intervalRows) {
        this.fileName     = fileName;
        this.intervalRows = intervalRows;
        this.startTime    = Instant.now();
    }

    /**
     * Called for every row written. Logs a progress line when the interval is reached.
     */
    public void tick() {
        rowCount++;
        if (rowCount % intervalRows == 0) {
            logProgress();
        }
    }

    /**
     * Called when all rows have been written. Always logs a final summary line.
     */
    public void done() {
        Duration elapsed = elapsed();
        long seconds = elapsed.toSeconds();
        long throughput = seconds > 0 ? rowCount / seconds : rowCount;

        log.info("  ✔ {} — {} rows written in {} ({} rows/sec)",
                fileName,
                formatCount(rowCount),
                formatDuration(elapsed),
                formatCount(throughput));
    }

    /**
     * Returns the total number of rows processed.
     */
    public long rowCount() {
        return rowCount;
    }

    // ─── Internal ────────────────────────────────────────────────────────────

    private void logProgress() {
        Duration elapsed = elapsed();
        long seconds = elapsed.toSeconds();
        long throughput = seconds > 0 ? rowCount / seconds : rowCount;

        log.info("  ↳ {} — {} rows processed... ({} rows/sec, elapsed: {})",
                fileName,
                formatCount(rowCount),
                formatCount(throughput),
                formatDuration(elapsed));
    }

    private Duration elapsed() {
        return Duration.between(startTime, Instant.now());
    }

    // ─── Formatting helpers ───────────────────────────────────────────────────

    /**
     * Formats a count with thousands separators for readability.
     * e.g. 1234567 → "1,234,567"
     */
    static String formatCount(long count) {
        return String.format("%,d", count);
    }

    /**
     * Formats a duration as a human-readable string.
     * e.g. PT65S → "1m 05s", PT3723S → "1h 02m 03s"
     */
    static String formatDuration(Duration duration) {
        long totalSeconds = duration.toSeconds();
        long hours   = totalSeconds / 3600;
        long minutes = (totalSeconds % 3600) / 60;
        long seconds = totalSeconds % 60;
        long millis  = duration.toMillisPart();

        if (hours > 0) {
            return String.format("%dh %02dm %02ds", hours, minutes, seconds);
        } else if (minutes > 0) {
            return String.format("%dm %02ds", minutes, seconds);
        } else if (seconds > 0) {
            return String.format("%d.%03ds", seconds, millis);
        } else {
            return String.format("%dms", duration.toMillis());
        }
    }
}

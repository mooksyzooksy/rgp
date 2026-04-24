package com.tizitec.parquet2csv;

import com.tizitec.parquet2csv.ParquetConverter.ConversionResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

/**
 * PicoCLI {@code @Command} implementation — the main entry point for the conversion pipeline.
 *
 * <p>Accepts one or more Parquet file paths or a directory and converts each file to a
 * CSV in the configured output directory. Delegates all reading and writing logic to
 * {@link ParquetConverter}; this class is responsible only for CLI binding, input
 * resolution, orchestration, and reporting.
 *
 * <p>Behaviour highlights:
 * <ul>
 *   <li>Directory inputs are scanned non-recursively for {@code .parquet} files.</li>
 *   <li>Files are processed sequentially; a failure on one file does not abort the batch.</li>
 *   <li>Exit code {@code 0} means all files succeeded; {@code 1} means at least one failed.</li>
 * </ul>
 */
@Command(
        name        = "parquet-to-csv",
        mixinStandardHelpOptions = true,
        version     = "1.0.0",
        description = {
                "",
                "Converts Parquet files to CSV, one CSV per file.",
                "Supports any Parquet schema — nested/complex fields are serialized as JSON strings.",
                ""
        },
        footer = {
                "",
                "Examples:",
                "  parquet-to-csv trades.parquet -o /output",
                "  parquet-to-csv /data/parquet-dir/ -o /output --overwrite",
                "  parquet-to-csv a.parquet b.parquet -o /output --progress-interval 50000",
                ""
        }
)
public class ConvertCommand implements Callable<Integer> {

    private static final Logger log = LoggerFactory.getLogger(ConvertCommand.class);

    // ─── Inputs ──────────────────────────────────────────────────────────────

    @Parameters(
            arity      = "1..*",
            paramLabel = "<input>",
            description = "One or more Parquet files, or a directory containing .parquet files."
    )
    private List<Path> inputs;

    // ─── Options ─────────────────────────────────────────────────────────────

    @Option(
            names       = {"-o", "--output-dir"},
            required    = true,
            paramLabel  = "<dir>",
            description = "Directory where CSV files will be written."
    )
    private Path outputDir;

    @Option(
            names       = {"--overwrite"},
            description = "Overwrite existing CSV files. Default: ${DEFAULT-VALUE}."
    )
    private boolean overwrite = false;

    @Option(
            names       = {"--null-value"},
            paramLabel  = "<string>",
            description = "String to use for null values. Default: empty string."
    )
    private String nullValue = "";

    @Option(
            names       = {"--delimiter"},
            paramLabel  = "<char>",
            description = "CSV column delimiter. Default: comma."
    )
    private char delimiter = ',';

    @Option(
            names       = {"--progress-interval"},
            paramLabel  = "<rows>",
            description = "Log a progress line every N rows. Default: ${DEFAULT-VALUE}."
    )
    private long progressInterval = 100_000L;

    @Option(
            names       = {"--skip-complex"},
            description = "Skip nested/complex fields (RECORD, ARRAY, MAP) instead of serializing them as JSON. Faster for flat data extraction. Default: ${DEFAULT-VALUE}."
    )
    private boolean skipComplex = false;

    // ─── Execution ───────────────────────────────────────────────────────────

    /**
     * Executes the conversion batch.
     *
     * <p>Resolves all input paths, creates the output directory if needed, converts each
     * Parquet file in order, and prints a batch summary at the end.
     *
     * @return {@code 0} if all files converted successfully, {@code 1} if any file failed
     */
    @Override
    public Integer call() {
        Instant batchStart = Instant.now();

        try {
            ensureOutputDirExists();
        } catch (IOException e) {
            log.error("Cannot create output directory '{}': {}", outputDir, e.getMessage());
            return 1;
        }

        List<Path> parquetFiles = resolveParquetFiles();

        if (parquetFiles.isEmpty()) {
            log.warn("No .parquet files found in the provided inputs.");
            return 0;
        }

        int total = parquetFiles.size();
        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        log.info("  parquet-to-csv  |  {} file(s) queued", total);
        log.info("  Output dir      : {}", outputDir.toAbsolutePath());
        log.info("  Overwrite       : {}", overwrite);
        log.info("  Delimiter       : '{}'", delimiter);
        log.info("  Null value      : '{}'", nullValue.isEmpty() ? "<empty>" : nullValue);
        log.info("  Progress every  : {:,} rows", progressInterval);
        log.info("  Skip complex    : {}", skipComplex);
        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

        ConversionConfig   config    = new ConversionConfig(outputDir, overwrite, nullValue, delimiter, progressInterval, skipComplex);
        ParquetConverter   converter = new ParquetConverter(config);

        List<ConversionResult> successes = new ArrayList<>();
        List<String>           failures  = new ArrayList<>();

        for (int i = 0; i < total; i++) {
            Path parquetFile = parquetFiles.get(i);
            log.info("");
            log.info("[{}/{}] {}", i + 1, total, parquetFile.getFileName());

            try {
                ConversionResult result = converter.convert(parquetFile);
                successes.add(result);
            } catch (Exception e) {
                log.error("✘ Failed: {} — {}", parquetFile.getFileName(), e.getMessage());
                log.debug("  Stack trace:", e);
                failures.add(parquetFile.getFileName().toString());
            }
        }

        // ── Batch summary ─────────────────────────────────────────────────────
        Duration batchElapsed = Duration.between(batchStart, Instant.now());
        printBatchSummary(successes, failures, batchElapsed);

        return failures.isEmpty() ? 0 : 1;
    }

    // ─── Batch summary ────────────────────────────────────────────────────────

    /**
     * Prints a structured summary of the entire batch run to the log.
     * Always called at the end of {@link #call()}, even if all files failed.
     *
     * @param successes    list of successful conversion results, in processing order
     * @param failures     list of file names that failed to convert
     * @param batchElapsed total wall-clock duration of the batch
     */
    private void printBatchSummary(List<ConversionResult> successes,
                                   List<String> failures,
                                   Duration batchElapsed) {

        long totalRows = successes.stream().mapToLong(ConversionResult::rowCount).sum();

        log.info("");
        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        log.info("  BATCH SUMMARY");
        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        log.info("  Total time   : {}", ProgressLogger.formatDuration(batchElapsed));
        log.info("  Succeeded    : {}", successes.size());
        log.info("  Failed       : {}", failures.size());
        log.info("  Total rows   : {}", ProgressLogger.formatCount(totalRows));

        if (!successes.isEmpty()) {
            log.info("");
            log.info("  Converted files:");
            for (ConversionResult r : successes) {
                log.info("    ✔ {} — {:,} rows, {} cols, {}",
                        r.outputPath().getFileName(),
                        r.rowCount(),
                        r.columnCount(),
                        ProgressLogger.formatDuration(r.elapsed()));
            }
        }

        if (!failures.isEmpty()) {
            log.info("");
            log.info("  Failed files:");
            for (String name : failures) {
                log.info("    ✘ {}", name);
            }
        }

        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    /**
     * Resolves all CLI inputs into a deduplicated, sorted list of {@code .parquet} files.
     * Directories are listed non-recursively. Non-existent paths and non-Parquet files
     * are skipped with a warning rather than causing a hard failure.
     *
     * @return sorted list of resolved Parquet file paths, never {@code null}
     */
    private List<Path> resolveParquetFiles() {
        return inputs.stream()
                .flatMap(input -> {
                    if (!Files.exists(input)) {
                        log.warn("Input path does not exist, skipping: {}", input);
                        return Stream.empty();
                    }
                    if (Files.isDirectory(input)) {
                        return listParquetInDirectory(input);
                    }
                    if (isParquetFile(input)) {
                        return Stream.of(input);
                    }
                    log.warn("Not a .parquet file, skipping: {}", input);
                    return Stream.empty();
                })
                .distinct()
                .sorted()
                .toList();
    }

    /**
     * Lists all {@code .parquet} files directly inside {@code dir} (non-recursive).
     * Returns an empty stream and logs a warning if the directory cannot be read.
     *
     * @param dir the directory to scan
     * @return stream of {@code .parquet} file paths found inside {@code dir}
     */
    private Stream<Path> listParquetInDirectory(Path dir) {
        try {
            return Files.list(dir)
                    .filter(Files::isRegularFile)
                    .filter(this::isParquetFile)
                    .sorted();
        } catch (IOException e) {
            log.error("Cannot list directory '{}': {}", dir, e.getMessage());
            return Stream.empty();
        }
    }

    private boolean isParquetFile(Path path) {
        return path.getFileName().toString().toLowerCase().endsWith(".parquet");
    }

    /**
     * Creates the output directory (including any missing parent directories) if it does
     * not already exist. No-op if the directory is already present.
     *
     * @throws IOException if the directory cannot be created
     */
    private void ensureOutputDirExists() throws IOException {
        if (!Files.exists(outputDir)) {
            Files.createDirectories(outputDir);
            log.info("Created output directory: {}", outputDir);
        }
    }
}

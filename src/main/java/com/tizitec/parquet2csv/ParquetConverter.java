package com.tizitec.parquet2csv;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Core conversion logic. Reads any Parquet file using the Avro generic reader
 * (schema-driven, no POJOs required) and writes a CSV file.
 *
 * <p>Type handling strategy:
 * <ul>
 *   <li>Primitive types (string, int, long, float, double, boolean, bytes, enum) → toString()</li>
 *   <li>Logical types (date, time, timestamp, decimal, uuid) → toString() via Avro's conversion</li>
 *   <li>Complex types (record, array, map, union with nested record) → JSON string</li>
 *   <li>Null → configurable null placeholder (default: empty string)</li>
 * </ul>
 */
public class ParquetConverter {

    private static final Logger log = LoggerFactory.getLogger(ParquetConverter.class);

    private final ConversionConfig config;
    private final ObjectMapper objectMapper;
    private final Configuration hadoopConfig;

    public ParquetConverter(ConversionConfig config) {
        this.config       = config;
        this.objectMapper = new ObjectMapper();
        this.hadoopConfig = buildMinimalHadoopConfig();
    }

    /**
     * Converts a single Parquet file to a CSV file in the configured output directory.
     *
     * @param parquetFile path to the source .parquet file
     * @return result object carrying stats (row count, columns, duration, output path)
     * @throws IOException if reading or writing fails
     */
    public ConversionResult convert(Path parquetFile) throws IOException {
        Instant start   = Instant.now();
        Path    csvFile = resolveCsvOutputPath(parquetFile);
        String  name    = parquetFile.getFileName().toString();

        // ── Pre-flight checks ─────────────────────────────────────────────────
        if (Files.exists(csvFile) && !config.overwrite()) {
            throw new FileAlreadyExistsException(
                    csvFile.toString(), null,
                    "CSV already exists. Use --overwrite to replace it."
            );
        }

        long inputBytes = Files.exists(parquetFile) ? Files.size(parquetFile) : -1;
        log.info("┌─ Starting  : {}", name);
        log.info("│  Input     : {}", parquetFile.toAbsolutePath());
        log.info("│  Output    : {}", csvFile.toAbsolutePath());
        log.info("│  File size : {}", formatBytes(inputBytes));

        // ── Open Parquet reader ───────────────────────────────────────────────
        org.apache.hadoop.fs.Path hadoopPath = toHadoopPath(parquetFile);

        try (ParquetReader<GenericRecord> reader = AvroParquetReader
                .<GenericRecord>builder(HadoopInputFile.fromPath(hadoopPath, hadoopConfig))
                .withConf(hadoopConfig)
                .build()) {

            // Read the first record to bootstrap the schema
            GenericRecord first = reader.read();

            if (first == null) {
                log.warn("│  ⚠ File is empty — writing header-only CSV.");
                Files.createFile(csvFile);
                Duration elapsed = Duration.between(start, Instant.now());
                log.info("└─ Done ({}) — 0 rows written.", ProgressLogger.formatDuration(elapsed));
                return new ConversionResult(csvFile, 0, 0, elapsed);
            }

            Schema       schema  = first.getSchema();
            List<String> headers = extractHeaders(schema);
            int          cols    = headers.size();

            log.info("│  Columns   : {} → {}", cols, headers);
            log.info("│  Progress  : every {:,} rows", config.progressInterval());

            CSVFormat      csvFormat = buildCsvFormat(headers);
            ProgressLogger progress  = new ProgressLogger(name, config.progressInterval());

            // ── Write CSV ─────────────────────────────────────────────────────
            try (BufferedWriter writer = Files.newBufferedWriter(csvFile, StandardCharsets.UTF_8);
                 CSVPrinter printer = new CSVPrinter(writer, csvFormat)) {

                // Write the first record (already read to extract schema)
                printer.printRecord(toStringValues(first, schema));
                progress.tick();

                GenericRecord record;
                while ((record = reader.read()) != null) {
                    printer.printRecord(toStringValues(record, schema));
                    progress.tick();
                }
            }

            // ── Final summary ─────────────────────────────────────────────────
            progress.done();

            Duration elapsed     = Duration.between(start, Instant.now());
            long     outputBytes = Files.exists(csvFile) ? Files.size(csvFile) : -1;

            log.info("│  CSV size  : {}", formatBytes(outputBytes));
            log.info("└─ Completed : {} in {}",
                    name, ProgressLogger.formatDuration(elapsed));

            return new ConversionResult(csvFile, progress.rowCount(), cols, elapsed);
        }
    }

    // ─── Result record ────────────────────────────────────────────────────────

    /**
     * Carries per-file conversion stats back to the command layer.
     */
    public record ConversionResult(
            Path     outputPath,
            long     rowCount,
            int      columnCount,
            Duration elapsed
    ) {}

    // ─── Schema extraction ────────────────────────────────────────────────────

    private List<String> extractHeaders(Schema schema) {
        return schema.getFields()
                .stream()
                .filter(f -> !shouldSkip(f.schema()))
                .map(Schema.Field::name)
                .toList();
    }

    /**
     * Returns true if the field should be excluded because it is a complex type
     * (RECORD, ARRAY, MAP) and --skip-complex is enabled.
     * Nullable unions wrapping a complex type are also caught here.
     */
    private boolean shouldSkip(Schema fieldSchema) {
        if (!config.skipComplex()) return false;
        Schema resolved = resolveSchema(fieldSchema);
        return switch (resolved.getType()) {
            case RECORD, ARRAY, MAP -> true;
            default                 -> false;
        };
    }

    // ─── Record serialization ─────────────────────────────────────────────────

    private List<Object> toStringValues(GenericRecord record, Schema schema) {
        List<Object> values = new ArrayList<>(schema.getFields().size());
        for (Schema.Field field : schema.getFields()) {
            if (shouldSkip(field.schema())) continue;
            Object raw = record.get(field.name());
            values.add(serializeField(raw, field.schema()));
        }
        return values;
    }

    /**
     * Serializes a single Parquet field value to a {@code String} suitable for CSV output.
     *
     * <p>Dispatch order:
     * <ol>
     *   <li>If value is {@code null} → returns the configured null placeholder.</li>
     *   <li>Resolves nullable unions to their non-null branch.</li>
     *   <li>RECORD / ARRAY / MAP → JSON string via Jackson (or skipped if
     *       {@link ConversionConfig#skipComplex()} is {@code true}, but note: skipping
     *       is handled upstream in {@link #toStringValues}; this method is not called
     *       for skipped fields).</li>
     *   <li>All other types → {@code Object.toString()}.</li>
     * </ol>
     *
     * @param value       the raw field value from the Avro {@code GenericRecord}
     * @param fieldSchema the Avro schema of the field, used to determine serialization strategy
     * @return a non-null string representation of the value
     */
    private String serializeField(Object value, Schema fieldSchema) {
        if (value == null) {
            return config.nullValue();
        }

        Schema resolved = resolveSchema(fieldSchema);

        return switch (resolved.getType()) {
            case NULL   -> config.nullValue();
            case RECORD -> toJson(value);
            case ARRAY  -> toJson(value);
            case MAP    -> toJson(value);
            case UNION  -> {
                Schema runtimeSchema = resolveUnionType(value, resolved);
                yield serializeField(value, runtimeSchema);
            }
            // STRING, INT, LONG, FLOAT, DOUBLE, BOOLEAN, ENUM, BYTES, FIXED
            default -> value.toString();
        };
    }

    /**
     * Unwraps a nullable union schema to its non-null branch.
     *
     * <p>Parquet nullable fields are represented in Avro as a two-branch union
     * {@code ["null", "ActualType"]}. This method detects that pattern and returns
     * the non-null branch directly, allowing callers to dispatch on the real type.
     * For non-union schemas or complex multi-branch unions, the schema is returned as-is.
     *
     * @param schema the field schema, potentially a union
     * @return the resolved non-null schema, or the original schema if resolution is not applicable
     */
    private Schema resolveSchema(Schema schema) {
        if (schema.getType() != Schema.Type.UNION) {
            return schema;
        }
        List<Schema> types = schema.getTypes();
        if (types.size() == 2) {
            return types.stream()
                    .filter(s -> s.getType() != Schema.Type.NULL)
                    .findFirst()
                    .orElse(schema);
        }
        return schema;
    }

    /**
     * Determines the correct Avro branch schema for a value inside a complex multi-branch union.
     *
     * <p>Used as a fallback when {@link #resolveSchema} cannot simplify the union (i.e. more
     * than two branches). Matches the runtime Java type of {@code value} against known Avro
     * type mappings: {@code GenericRecord} → RECORD, {@code List} → ARRAY,
     * {@code Map} → MAP, {@code null} → NULL. Falls back to {@code STRING} if no branch matches.
     *
     * @param value       the runtime value whose type must be matched
     * @param unionSchema the multi-branch union schema to resolve against
     * @return the matching branch schema, or a synthetic STRING schema as a last resort
     */
    private Schema resolveUnionType(Object value, Schema unionSchema) {
        for (Schema branch : unionSchema.getTypes()) {
            if (branch.getType() == Schema.Type.NULL   && value == null)                       return branch;
            if (value instanceof GenericRecord         && branch.getType() == Schema.Type.RECORD) return branch;
            if (value instanceof List<?>               && branch.getType() == Schema.Type.ARRAY)  return branch;
            if (value instanceof java.util.Map<?, ?>  && branch.getType() == Schema.Type.MAP)    return branch;
        }
        return Schema.create(Schema.Type.STRING);
    }

    // ─── JSON serialization ───────────────────────────────────────────────────

    /**
     * Serializes a complex Avro value (GenericRecord, GenericArray, or Map) to a JSON string
     * using Jackson's {@code ObjectMapper}.
     *
     * <p>Avro's collection types are Jackson-serializable out of the box. Falls back to
     * {@code Object.toString()} if Jackson throws, logging a warning.
     *
     * @param value the complex field value to serialize
     * @return a JSON string representation, never {@code null}
     */
    private String toJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (Exception e) {
            log.warn("│  ⚠ Could not serialize field to JSON, falling back to toString(): {}", e.getMessage());
            return value.toString();
        }
    }

    // ─── CSV formatting ───────────────────────────────────────────────────────

    /**
     * Builds the Apache Commons CSV format used for writing.
     * Header, delimiter, record separator, and null string are all driven by
     * {@link ConversionConfig}.
     *
     * @param headers ordered list of column names derived from the Parquet schema
     * @return configured {@code CSVFormat} instance
     */
    private CSVFormat buildCsvFormat(List<String> headers) {
        return CSVFormat.DEFAULT.builder()
                .setHeader(headers.toArray(String[]::new))
                .setDelimiter(config.delimiter())
                .setRecordSeparator(System.lineSeparator())
                .setNullString(config.nullValue())
                .build();
    }

    // ─── Path helpers ─────────────────────────────────────────────────────────

    /**
     * Derives the output CSV {@link Path} for a given Parquet input file.
     * The base name is preserved and the {@code .parquet} extension is replaced with {@code .csv}.
     * The resulting path is placed directly inside the configured output directory.
     *
     * @param parquetFile the source Parquet file
     * @return the target CSV file path (not yet created)
     */
    private Path resolveCsvOutputPath(Path parquetFile) {
        String originalName = parquetFile.getFileName().toString();
        String csvName      = originalName.replaceAll("(?i)\\.parquet$", ".csv");
        return config.outputDir().resolve(csvName);
    }

    /**
     * Converts a Java NIO {@link Path} to a Hadoop {@code Path} via its URI,
     * ensuring the Hadoop filesystem abstraction can locate the local file.
     *
     * @param path the NIO path to convert
     * @return equivalent Hadoop path
     */
    private org.apache.hadoop.fs.Path toHadoopPath(Path path) {
        return new org.apache.hadoop.fs.Path(path.toAbsolutePath().toUri());
    }

    // ─── Formatting helpers ───────────────────────────────────────────────────

    /**
     * Formats a byte count as a human-readable string with an appropriate unit suffix.
     * Uses binary units (1 KB = 1,024 bytes).
     *
     * <p>Examples: {@code 512} → {@code "512 B"}, {@code 1536} → {@code "1.5 KB"},
     * {@code 10485760} → {@code "10.0 MB"}.
     *
     * @param bytes the byte count; negative values return {@code "unknown"}
     * @return a formatted string such as {@code "142.3 MB"}
     */
    static String formatBytes(long bytes) {
        if (bytes < 0)             return "unknown";
        if (bytes < 1_024)         return bytes + " B";
        if (bytes < 1_048_576)     return String.format("%.1f KB", bytes / 1_024.0);
        if (bytes < 1_073_741_824) return String.format("%.1f MB", bytes / 1_048_576.0);
        return                            String.format("%.2f GB", bytes / 1_073_741_824.0);
    }

    // ─── Hadoop configuration ─────────────────────────────────────────────────

    /**
     * Builds a minimal Hadoop {@link Configuration} scoped to local filesystem access only.
     *
     * <p>Passing {@code false} to the {@code Configuration} constructor prevents loading
     * of any default XML files ({@code core-default.xml}, {@code core-site.xml}), which
     * avoids spurious warnings about missing HDFS, YARN, or native library resources.
     * Only the settings required to read local files via {@code file:///} are set.
     *
     * @return a lightweight Hadoop configuration for local file I/O
     */
    private Configuration buildMinimalHadoopConfig() {
        Configuration conf = new Configuration(false); // false = no default XML loading
        conf.set("fs.defaultFS",               "file:///");
        conf.set("fs.file.impl",               org.apache.hadoop.fs.LocalFileSystem.class.getName());
        conf.set("fs.file.impl.disable.cache", "true");
        conf.set("io.native.lib.available",    "false");
        return conf;
    }
}

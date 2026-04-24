package com.tizitec.parquet2csv;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
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
 * Core conversion logic. Reads any Parquet file using the low-level
 * {@link ParquetFileReader} API with a {@link GroupRecordConverter} —
 * fully generic, schema-driven, and free of Hadoop/MapReduce dependencies.
 *
 * <p>Type handling strategy:
 * <ul>
 *   <li>Primitive types (string, int, long, float, double, boolean, binary) → toString()</li>
 *   <li>Complex types (group/struct, list, map) → JSON string via Jackson, or skipped
 *       if {@link ConversionConfig#skipComplex()} is {@code true}</li>
 *   <li>Null / missing fields → configurable null placeholder (default: empty string)</li>
 * </ul>
 *
 * <p>Uses {@link LocalInputFile} for all file I/O — pure Java NIO,
 * no {@code HADOOP_HOME}, no {@code winutils.exe} required on any platform.
 */
public class ParquetConverter {

    private static final Logger log = LoggerFactory.getLogger(ParquetConverter.class);

    private final ConversionConfig config;

    public ParquetConverter(ConversionConfig config) {
        this.config = config;
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

        // ── Pre-flight ────────────────────────────────────────────────────────
        if (Files.exists(csvFile) && !config.overwrite()) {
            throw new FileAlreadyExistsException(
                    csvFile.toString(), null,
                    "CSV already exists. Use --overwrite to replace it."
            );
        }

        long inputBytes = Files.size(parquetFile);
        log.info("┌─ Starting  : {}", name);
        log.info("│  Input     : {}", parquetFile.toAbsolutePath());
        log.info("│  Output    : {}", csvFile.toAbsolutePath());
        log.info("│  File size : {}", formatBytes(inputBytes));

        // ── Open via pure-Java LocalInputFile (no Hadoop filesystem) ──────────
        try (ParquetFileReader fileReader = ParquetFileReader.open(new LocalInputFile(parquetFile))) {

            ParquetMetadata metadata   = fileReader.getFooter();
            MessageType     schema     = metadata.getFileMetaData().getSchema();
            List<String>    headers    = extractHeaders(schema);
            int             cols       = headers.size();
            long            totalRows  = fileReader.getRecordCount();

            if (totalRows == 0) {
                log.warn("│  ⚠ File is empty — writing header-only CSV.");
                writeHeaderOnlyCsv(csvFile, headers);
                Duration elapsed = Duration.between(start, Instant.now());
                log.info("└─ Done ({}) — 0 rows.", ProgressLogger.formatDuration(elapsed));
                return new ConversionResult(csvFile, 0, cols, elapsed);
            }

            log.info("│  Columns   : {} → {}", cols, headers);
            log.info("│  Rows      : {}", ProgressLogger.formatCount(totalRows));
            log.info("│  Progress  : every {:,} rows", config.progressInterval());

            CSVFormat      csvFormat = buildCsvFormat(headers);
            ProgressLogger progress  = new ProgressLogger(name, config.progressInterval());

            // ── Iterate row groups → pages → records ──────────────────────────
            try (BufferedWriter writer = Files.newBufferedWriter(csvFile, StandardCharsets.UTF_8);
                 CSVPrinter printer = new CSVPrinter(writer, csvFormat)) {

                ColumnIOFactory  ioFactory = new ColumnIOFactory();
                PageReadStore    pages;

                while ((pages = fileReader.readNextRowGroup()) != null) {
                    long         rowsInGroup = pages.getRowCount();
                    MessageColumnIO columnIO = ioFactory.getColumnIO(schema);
                    RecordReader<Group> recordReader = columnIO.getRecordReader(
                            pages, new GroupRecordConverter(schema)
                    );

                    for (long i = 0; i < rowsInGroup; i++) {
                        Group record = recordReader.read();
                        printer.printRecord(toStringValues(record, schema));
                        progress.tick();
                    }
                }
            }

            // ── Summary ───────────────────────────────────────────────────────
            progress.done();

            Duration elapsed     = Duration.between(start, Instant.now());
            long     outputBytes = Files.size(csvFile);

            log.info("│  CSV size  : {}", formatBytes(outputBytes));
            log.info("└─ Completed : {} in {}", name, ProgressLogger.formatDuration(elapsed));

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

    /**
     * Extracts the ordered list of column names from the Parquet schema.
     * Complex fields are included unless {@link ConversionConfig#skipComplex()} is set.
     *
     * @param schema the Parquet message schema read from the file footer
     * @return ordered list of column names for the CSV header
     */
    private List<String> extractHeaders(MessageType schema) {
        return schema.getFields().stream()
                .filter(f -> !shouldSkip(f))
                .map(Type::getName)
                .toList();
    }

    /**
     * Returns true if the field should be excluded because it is a complex/group type
     * and {@link ConversionConfig#skipComplex()} is enabled.
     *
     * @param field the Parquet schema field to check
     * @return true if the field should be omitted from output
     */
    private boolean shouldSkip(Type field) {
        return config.skipComplex() && !field.isPrimitive();
    }

    // ─── Record serialization ─────────────────────────────────────────────────

    /**
     * Converts a Parquet {@link Group} (one row) into an ordered list of String values,
     * one per included column, in schema field order.
     *
     * @param group  the row record read from the Parquet file
     * @param schema the message schema defining field order and types
     * @return ordered list of serialized field values
     */
    private List<Object> toStringValues(Group group, MessageType schema) {
        List<Object> values = new ArrayList<>(schema.getFieldCount());

        for (Type field : schema.getFields()) {
            if (shouldSkip(field)) continue;

            String fieldName = field.getName();
            int    fieldIdx  = schema.getFieldIndex(fieldName);

            // Check if the field has any value in this row (handles nulls/optional fields)
            int repetitionCount = group.getFieldRepetitionCount(fieldIdx);

            if (repetitionCount == 0) {
                values.add(config.nullValue());
                continue;
            }

            values.add(serializeField(group, field, fieldIdx));
        }
        return values;
    }

    /**
     * Serializes a single field value from a {@link Group} row to a String.
     *
     * <p>Primitive types are read with the appropriate typed accessor for accuracy.
     * Complex (group) types are serialized as a JSON string via Jackson.
     *
     * @param group    the row containing the field
     * @param field    the schema definition of the field
     * @param fieldIdx the integer index of the field within the schema
     * @return a non-null string representation, or the configured null placeholder
     */
    private String serializeField(Group group, Type field, int fieldIdx) {
        try {
            if (field.isPrimitive()) {
                return serializePrimitive(group, field, fieldIdx);
            } else {
                // Complex group type — use Parquet's own toString() representation
                Group nested = group.getGroup(fieldIdx, 0);
                return toJson(nested.toString());
            }
        } catch (Exception e) {
            log.warn("│  ⚠ Could not read field '{}': {} — using null value",
                    field.getName(), e.getMessage());
            return config.nullValue();
        }
    }

    /**
     * Reads a primitive Parquet field using the appropriate typed accessor.
     * Falls back to the generic {@code group.getValueToString()} if the
     * primitive type is not explicitly handled.
     *
     * @param group    the row containing the field
     * @param field    the schema field (must be primitive)
     * @param fieldIdx the integer index of the field within the schema
     * @return string representation of the primitive value
     */
    private String serializePrimitive(Group group, Type field, int fieldIdx) {
        org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName typeName =
                field.asPrimitiveType().getPrimitiveTypeName();

        // getValueToString() is the universal safe accessor on Group —
        // it handles all primitive types including INT96 and BINARY correctly.
        // For well-known types we use typed accessors for cleaner output.
        return switch (typeName) {
            case INT32   -> String.valueOf(group.getInteger(fieldIdx, 0));
            case INT64   -> String.valueOf(group.getLong(fieldIdx, 0));
            case FLOAT   -> String.valueOf(group.getFloat(fieldIdx, 0));
            case DOUBLE  -> String.valueOf(group.getDouble(fieldIdx, 0));
            case BOOLEAN -> String.valueOf(group.getBoolean(fieldIdx, 0));
            // BINARY covers UTF8 strings, ENUMs, DECIMAL-as-bytes, JSON, BSON
            // FIXED_LEN_BYTE_ARRAY covers fixed decimals, UUIDs
            // INT96 is a legacy 12-byte timestamp (Spark/Hive era)
            // All three: getValueToString() handles them safely without deprecated APIs
            case BINARY, FIXED_LEN_BYTE_ARRAY, INT96 ->
                    group.getValueToString(fieldIdx, 0);
        };
    }

    // ─── JSON serialization ───────────────────────────────────────────────────

    /**
     * Serializes a complex Parquet Group field to a string for CSV output.
     * Uses Parquet's own {@code Group.toString()} which produces a structured
     * text representation — no external JSON library needed.
     *
     * @param value the string representation of the complex Group value
     * @return the value wrapped in quotes for CSV safety, never {@code null}
     */
    private String toJson(String value) {
        // Escape any double-quotes inside the value so CSV stays valid
        return value.replace(""", """");
    }

    // ─── CSV helpers ──────────────────────────────────────────────────────────

    /**
     * Builds the Apache Commons CSV format driven by {@link ConversionConfig}.
     *
     * @param headers ordered column names for the CSV header row
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

    /**
     * Writes a header-only CSV file for an empty Parquet input.
     *
     * @param csvFile the target CSV path
     * @param headers the column names to write as the header row
     * @throws IOException if the file cannot be written
     */
    private void writeHeaderOnlyCsv(Path csvFile, List<String> headers) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(csvFile, StandardCharsets.UTF_8);
             CSVPrinter printer = new CSVPrinter(writer, buildCsvFormat(headers))) {
            // header is written automatically by CSVFormat; no rows to add
        }
    }

    // ─── Path helpers ─────────────────────────────────────────────────────────

    /**
     * Derives the output CSV path: same base name as the input, {@code .parquet} → {@code .csv},
     * placed in the configured output directory.
     *
     * @param parquetFile the source Parquet file
     * @return the target CSV path (not yet created)
     */
    private Path resolveCsvOutputPath(Path parquetFile) {
        String originalName = parquetFile.getFileName().toString();
        String csvName      = originalName.replaceAll("(?i)\\.parquet$", ".csv");
        return config.outputDir().resolve(csvName);
    }

    // ─── Formatting helpers ───────────────────────────────────────────────────

    /**
     * Formats a byte count as a human-readable string with an appropriate unit suffix.
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
}

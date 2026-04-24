# parquet-to-csv

A generic CLI tool to convert Parquet files to CSV, built with Java 21 and PicoCLI.

Any Parquet schema is supported — primitive fields map directly to CSV columns, nested/complex
fields (structs, arrays, maps) are serialized as JSON strings by default, or skipped entirely
with `--skip-complex` for faster flat-data extraction.

---

## Requirements

- Java 21+
- Maven 3.8+ (to build)

---

## Build

```bash
mvn package -DskipTests
```

Output: `target/parquet-to-csv-1.0.0.jar` — fat JAR, no external dependencies needed at runtime.

---

## Usage

```bash
java -jar target/parquet-to-csv-1.0.0.jar [OPTIONS] <input>...
```

### Arguments

| Argument  | Description                                                  |
|-----------|--------------------------------------------------------------|
| `<input>` | One or more `.parquet` files, or a directory containing them |

### Options

| Option                    | Default      | Description                                                                 |
|---------------------------|--------------|-----------------------------------------------------------------------------|
| `-o`, `--output-dir`      | —            | **Required.** Directory where CSV files will be written                     |
| `--overwrite`             | `false`      | Overwrite existing CSV files                                                |
| `--skip-complex`          | `false`      | Skip RECORD/ARRAY/MAP fields instead of serializing them as JSON strings    |
| `--delimiter <char>`      | `,`          | CSV column delimiter                                                         |
| `--null-value <string>`   | `""`         | String written for null values                                              |
| `--progress-interval <n>` | `100000`     | Log a progress line every N rows                                            |
| `-V`, `--version`         |              | Print version and exit                                                      |
| `-h`, `--help`            |              | Print help and exit                                                         |

---

## Examples

```bash
# Single file
java -jar parquet-to-csv-1.0.0.jar trades.parquet -o /output

# Multiple files
java -jar parquet-to-csv-1.0.0.jar file1.parquet file2.parquet -o /output

# Entire directory, overwrite existing CSVs
java -jar parquet-to-csv-1.0.0.jar /data/parquet-files/ -o /output --overwrite

# Skip nested fields for faster extraction of flat data
java -jar parquet-to-csv-1.0.0.jar /data/ -o /output --skip-complex

# Semicolon delimiter, custom null placeholder, progress every 50k rows
java -jar parquet-to-csv-1.0.0.jar file.parquet -o /output \
  --delimiter ';' --null-value "N/A" --progress-interval 50000
```

---

## Type mapping

| Parquet / Avro type              | CSV output                                  |
|----------------------------------|---------------------------------------------|
| string, int, long, float, double | Direct string representation                |
| boolean                          | `true` / `false`                            |
| date, time, timestamp            | ISO-8601 string (via Avro logical types)    |
| decimal                          | Precision-aware string                      |
| enum                             | Enum symbol string                          |
| bytes / fixed                    | `toString()`                                |
| **record (struct)**              | **JSON object string** (or skipped)         |
| **array / list**                 | **JSON array string** (or skipped)          |
| **map**                          | **JSON object string** (or skipped)         |
| null                             | Configured `--null-value` (default: empty)  |
| nullable union `["null", T]`     | Null placeholder, or unwrapped T if present |

> Use `--skip-complex` to drop RECORD, ARRAY, and MAP columns entirely instead of
> serializing them as JSON. Useful when you only need flat scalar data and want
> maximum throughput.

---

## What the output looks like

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  parquet-to-csv  |  2 file(s) queued
  Output dir      : /output
  Overwrite       : false
  Delimiter       : ','
  Null value      : '<empty>'
  Progress every  : 100,000 rows
  Skip complex    : false
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

[1/2] trades.parquet
┌─ Starting  : trades.parquet
│  Input     : /data/trades.parquet
│  File size : 142.3 MB
│  Columns   : 12 → [id, date, isin, quantity, ...]
│  Progress  : every 100,000 rows
  ↳ trades.parquet — 100,000 rows processed... (312,500 rows/sec, elapsed: 0.320s)
  ✔ trades.parquet — 147,832 rows written in 0.512s (288,734 rows/sec)
│  CSV size  : 89.7 MB
└─ Completed : trades.parquet in 0.512s

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  BATCH SUMMARY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Total time   : 1.1s
  Succeeded    : 2
  Failed       : 0
  Total rows   : 298,441

  Converted files:
    ✔ trades.csv    — 147,832 rows, 12 cols, 0.512s
    ✔ positions.csv — 150,609 rows,  8 cols, 0.601s
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

---

## Performance notes

- The read loop is streaming — one record at a time, no full-file buffering in memory.
  Heap usage stays flat regardless of file size.
- The main cost for large files is the Avro generic reader overhead and, for complex fields,
  Jackson JSON serialization. Use `--skip-complex` to eliminate the JSON cost entirely.
- Throughput on a flat schema with SSD storage is typically **200k–400k rows/sec**.
  Deeply nested schemas with many complex fields will be slower.
- The rows/sec figure shown in progress logs is your real-time performance indicator.

---

## Project structure

```
parquet-to-csv/
├── pom.xml
└── src/main/java/com/tizitec/parquet2csv/
    ├── Main.java               Entry point — wires PicoCLI and exits with its return code
    ├── ConvertCommand.java     CLI definition (@Command, @Option, @Parameters) and orchestration
    ├── ConversionConfig.java   Immutable config record — bridges CLI layer and converter
    ├── ParquetConverter.java   Core conversion logic — generic, schema-driven
    └── ProgressLogger.java     Row-count progress and throughput logging utility
└── src/main/resources/
    └── logback.xml             Logging config — suppresses Hadoop/Parquet/Avro noise
```

---

## Dependencies

| Library            | Version | Purpose                                              |
|--------------------|---------|------------------------------------------------------|
| picocli            | 4.7.6   | CLI framework                                        |
| parquet-avro       | 1.14.3  | Generic Parquet reader (schema-driven, no POJOs)     |
| avro               | 1.11.4  | Avro GenericRecord and Schema API                    |
| hadoop-common      | 3.4.1   | Required by Parquet's InputFile abstraction          |
| commons-csv        | 1.12.0  | CSV writing                                          |
| jackson-databind   | 2.17.2  | JSON serialization of complex Parquet fields         |
| logback-classic    | 1.5.8   | Logging implementation                               |

> **Note on Hadoop:** `hadoop-common` is a transitive requirement of the Parquet reader —
> used only for its local filesystem abstraction. No Hadoop cluster is needed or configured.

---

## Exit codes

| Code | Meaning                                                       |
|------|---------------------------------------------------------------|
| `0`  | All files converted successfully                              |
| `1`  | One or more files failed (remaining files are still processed)|

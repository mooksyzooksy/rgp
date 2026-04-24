package com.tizitec.parquet2csv;

import java.nio.file.Path;

/**
 * Immutable configuration passed from the CLI layer to the converter.
 * Keeps the converter free of PicoCLI dependencies.
 *
 * @param outputDir        directory where generated CSV files are written
 * @param overwrite        if {@code true}, existing CSV files are replaced silently;
 *                         if {@code false}, a {@link java.nio.file.FileAlreadyExistsException}
 *                         is thrown when a target CSV already exists
 * @param nullValue        string written to the CSV cell when a Parquet field value is {@code null};
 *                         defaults to empty string
 * @param delimiter        CSV column separator character; defaults to comma {@code ,}
 * @param progressInterval number of rows between each progress log line; defaults to 100,000
 * @param skipComplex      if {@code true}, fields of type RECORD, ARRAY, or MAP are silently
 *                         excluded from the output instead of being serialized as a JSON string;
 *                         useful for faster extraction of flat data from mixed-schema files
 */
public record ConversionConfig(
        Path    outputDir,
        boolean overwrite,
        String  nullValue,
        char    delimiter,
        long    progressInterval,
        boolean skipComplex
) {}

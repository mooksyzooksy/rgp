package com.tizitec.parquet2csv;

import picocli.CommandLine;

/**
 * Entry point. Delegates entirely to PicoCLI.
 * Exit code is propagated from the command execution result.
 */
public class Main {

    public static void main(String[] args) {
        int exitCode = new CommandLine(new ConvertCommand())
                .setCaseInsensitiveEnumValuesAllowed(true)
                .setUsageHelpAutoWidth(true)
                .execute(args);
        System.exit(exitCode);
    }
}

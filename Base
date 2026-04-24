package com.tizitec.parquet2csv;

import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Pure Java NIO implementation of Parquet's {@link InputFile} interface.
 *
 * <p>Replaces {@code HadoopInputFile} entirely — no {@code HADOOP_HOME},
 * no {@code winutils.exe}, no Hadoop filesystem abstraction. Reads directly
 * from the local filesystem via {@link SeekableByteChannel}, which is available
 * on all platforms (Linux, macOS, Windows) with no external dependencies.
 *
 * <p>This is the correct approach for a standalone local-file tool. The Hadoop
 * filesystem layer exists for distributed storage (HDFS, S3, GCS) and adds
 * no value — and significant friction — when reading local files.
 */
public class LocalInputFile implements InputFile {

    private final Path   path;
    private final long   length;

    /**
     * Creates a {@code LocalInputFile} for the given local file path.
     *
     * @param path path to the Parquet file; must exist and be readable
     * @throws IOException if the file size cannot be determined
     */
    public LocalInputFile(Path path) throws IOException {
        this.path   = path;
        this.length = Files.size(path);
    }

    /**
     * Returns the total length of the file in bytes.
     * Used by the Parquet reader to locate the file footer.
     *
     * @return file size in bytes
     */
    @Override
    public long getLength() {
        return length;
    }

    /**
     * Opens a new {@link SeekableInputStream} over the file.
     * The Parquet reader calls this to read row groups and the file footer.
     *
     * @return a new seekable input stream positioned at byte 0
     * @throws IOException if the file cannot be opened
     */
    @Override
    public SeekableInputStream newStream() throws IOException {
        return new NioSeekableInputStream(
                Files.newByteChannel(path, StandardOpenOption.READ)
        );
    }

    // ─── SeekableInputStream implementation ──────────────────────────────────

    /**
     * Adapts a {@link SeekableByteChannel} to Parquet's {@link SeekableInputStream} API.
     *
     * <p>Parquet's reader requires random-access reads (seek + read) to navigate
     * row groups and read the file footer. {@code SeekableByteChannel} provides
     * exactly this — it is Java NIO's standard seekable file abstraction.
     */
    private static final class NioSeekableInputStream extends SeekableInputStream {

        private final SeekableByteChannel channel;

        NioSeekableInputStream(SeekableByteChannel channel) {
            this.channel = channel;
        }

        // ── Position ─────────────────────────────────────────────────────────

        @Override
        public long getPos() throws IOException {
            return channel.position();
        }

        @Override
        public void seek(long newPos) throws IOException {
            channel.position(newPos);
        }

        // ── Byte-level reads ──────────────────────────────────────────────────

        @Override
        public int read() throws IOException {
            ByteBuffer buf = ByteBuffer.allocate(1);
            int n = channel.read(buf);
            return n == -1 ? -1 : buf.get(0) & 0xFF;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            ByteBuffer buf = ByteBuffer.wrap(b, off, len);
            return channel.read(buf);
        }

        // ── Fully-reading variants (required by Parquet) ──────────────────────

        @Override
        public void readFully(byte[] bytes) throws IOException {
            readFully(bytes, 0, bytes.length);
        }

        @Override
        public void readFully(byte[] bytes, int start, int len) throws IOException {
            ByteBuffer buf = ByteBuffer.wrap(bytes, start, len);
            while (buf.hasRemaining()) {
                int n = channel.read(buf);
                if (n == -1) {
                    throw new EOFException(
                            "Reached end of file before reading " + len + " bytes"
                    );
                }
            }
        }

        @Override
        public int read(ByteBuffer buf) throws IOException {
            return channel.read(buf);
        }

        @Override
        public void readFully(ByteBuffer buf) throws IOException {
            while (buf.hasRemaining()) {
                int n = channel.read(buf);
                if (n == -1) {
                    throw new EOFException("Reached end of file before filling ByteBuffer");
                }
            }
        }

        // ── Lifecycle ─────────────────────────────────────────────────────────

        @Override
        public void close() throws IOException {
            channel.close();
        }
    }
}

package uy.edu.ort.arqrealtime.collection.tier;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;


final class HybridMessageLogger {

    private static RocksDB transientStateDB = null;
    private static RocksDB failedStateDB = null;
    private static Options options = null;
    private static final Path transientPath = new File("state/transient").toPath();
    private static final Path failedPath = new File("state/failed").toPath();

    static void initialize() throws Exception {
        RocksDB.loadLibrary();
        options = new Options().setCreateIfMissing(true);
        ensureDirectories();
        transientStateDB = RocksDB.open(options, transientPath.toString());
        failedStateDB = RocksDB.open(options, failedPath.toString());
    }

    private static void ensureDirectories() throws IOException {
        if (Files.notExists(transientPath)) {
            Files.createDirectories(transientPath);
        }
        if (Files.notExists(failedPath)) {
            Files.createDirectories(failedPath);
        }
    }

    static void close() {
        if (null != transientStateDB) {
            transientStateDB.close();
            transientStateDB = null;
        }
        if (null != failedStateDB) {
            failedStateDB.close();
            failedStateDB = null;
        }

        if (null != options) {
            options.close();
            options = null;
        }
    }

    static void addEvent(final String eventKey, final byte[] eventData) throws Exception {
        final byte[] keyBytes = eventKey.getBytes(StandardCharsets.UTF_8);
        byte[] value = transientStateDB.get(keyBytes);
        if (value != null) {
            transientStateDB.put(keyBytes, eventData);
        }
    }

    static void removeEvent(final String eventKey) throws Exception {
        final byte[] keyBytes = eventKey.getBytes(StandardCharsets.UTF_8);
        transientStateDB.remove(keyBytes);
    }

    static void moveToFailed(final String eventKey) {
        try {
            final byte[] keyBytes = eventKey.getBytes(StandardCharsets.UTF_8);
            final byte[] eventBody = transientStateDB.get(keyBytes);
            if (null != eventBody) {
                failedStateDB.put(keyBytes, eventBody);
                transientStateDB.remove(keyBytes);
            }
        } catch (Exception ex) {
            //this should be logged and perhaps throw
        }
    }

}

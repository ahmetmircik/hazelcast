/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.cp.internal.raft.impl.persistence;

import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.log.LogEntry;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotChunk;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotEntry;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.HeapData;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/**
 * A self-contained, file-backed {@link RaftStateStore} implementation
 * (a working OSS replacement for the enterprise store, implementing the
 * snapshot-redesign durability contract).
 * <p>
 * Layout (one directory per Raft group):
 * <pre>
 *   &lt;dir&gt;/members.dat      local member + initial members (atomic write)
 *   &lt;dir&gt;/term.dat         term + voted-for (atomic write)
 *   &lt;dir&gt;/raft.log         append-only, length-prefixed serialized LogEntry records
 *   &lt;dir&gt;/snapshot-&lt;i&gt;/    published snapshot: chunk-&lt;n&gt;.dat files (atomic rename target)
 *   &lt;dir&gt;/staging-&lt;i&gt;/     in-progress snapshot chunks, renamed on {@link #flushLogs()}
 * </pre>
 * Records are length-prefixed serialized objects ({@code int length + bytes})
 * produced by the injected {@link SerializationService}, so any registered
 * object type (LogEntry, SnapshotChunk, endpoints) can be persisted. A torn
 * trailing record (process crash mid-write) is tolerated on load: the partial
 * tail is silently dropped.
 * <p>
 * Crash safety: log records are fsync'd by {@link #flushLogs()}; metadata is
 * written via temp-file + atomic rename + parent-dir fsync; snapshot
 * publication is a staging-directory atomic rename on {@link #flushLogs()}.
 */
public class FileRaftStateStore implements RaftStateStore, RaftStateLoader {

    private static final String MEMBERS_FILE = "members.dat";
    private static final String TERM_FILE = "term.dat";
    private static final String LOG_FILE = "raft.log";
    private static final String STAGING_PREFIX = "staging-";
    private static final String SNAPSHOT_PREFIX = "snapshot-";
    private static final String CHUNK_PREFIX = "chunk-";
    private static final String TMP_SUFFIX = ".tmp";

    /** Sanity cap for a single log record (512 MB). */
    private static final int MAX_RECORD_LENGTH = 1 << 29;

    private final Path dir;
    private final SerializationService serializationService;

    private RaftEndpoint localMember;
    private Collection<RaftEndpoint> initialMembers;
    private int term;
    private RaftEndpoint votedFor;

    private FileOutputStream logFileOut;
    private DataOutputStream logOut;
    private FileChannel logChannel;

    private long stagingSnapshotIndex = -1L;
    private Path stagingDir;
    private long publishedSnapshotIndex = -1L;

    public FileRaftStateStore(Path directory, SerializationService serializationService) {
        this.dir = directory;
        this.serializationService = serializationService;
    }

    @Override
    public void open() throws IOException {
        Files.createDirectories(dir);
        if (Files.exists(dir.resolve(LOG_FILE))) {
            loadMetadata();
        }
        logFileOut = new FileOutputStream(dir.resolve(LOG_FILE).toFile(), true);
        logOut = new DataOutputStream(new BufferedOutputStream(logFileOut));
        logChannel = logFileOut.getChannel();
    }

    private void loadMetadata() throws IOException {
        Path membersFile = dir.resolve(MEMBERS_FILE);
        if (Files.exists(membersFile)) {
            try (DataInputStream in = new DataInputStream(
                    new BufferedInputStream(new FileInputStream(membersFile.toFile())))) {
                localMember = (RaftEndpoint) deserialize(readBytes(in));
                initialMembers = (Collection<RaftEndpoint>) deserialize(readBytes(in));
            }
        }
        Path termFile = dir.resolve(TERM_FILE);
        if (Files.exists(termFile)) {
            try (DataInputStream in = new DataInputStream(
                    new BufferedInputStream(new FileInputStream(termFile.toFile())))) {
                term = in.readInt();
                votedFor = in.readBoolean() ? (RaftEndpoint) deserialize(readBytes(in)) : null;
            }
        }
    }

    private static byte[] readBytes(DataInputStream in) throws IOException {
        int length = in.readInt();
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return bytes;
    }

    @Override
    public synchronized void persistInitialMembers(@Nonnull RaftEndpoint localMember,
                                                   @Nonnull Collection<RaftEndpoint> initialMembers) throws IOException {
        this.localMember = localMember;
        this.initialMembers = initialMembers;
        atomicWrite(dir.resolve(MEMBERS_FILE), frame(localMember, new ArrayList<>(initialMembers)));
    }

    @Override
    public synchronized void persistTerm(int term, @Nullable RaftEndpoint votedFor) throws IOException {
        this.term = term;
        this.votedFor = votedFor;
        atomicWrite(dir.resolve(TERM_FILE), frame(term, votedFor));
    }

    @Override
    public synchronized void persistEntry(@Nonnull LogEntry entry) throws IOException {
        byte[] record = serialize(entry);
        logOut.writeInt(record.length);
        logOut.write(record);
    }

    @Override
    public synchronized void deleteEntriesFrom(long startIndexInclusive) throws IOException {
        // the buffered stream may hold records not yet flushed to the file
        logOut.flush();
        List<LogEntry> entries = readEntries();
        List<LogEntry> kept = new ArrayList<>();
        for (LogEntry entry : entries) {
            if (entry.index() < startIndexInclusive) {
                kept.add(entry);
            }
        }
        writeLogFile(kept);
    }

    @Override
    public synchronized void persistSnapshotChunk(Object snapshotChunk) throws IOException {
        SnapshotChunk chunk = (SnapshotChunk) snapshotChunk;
        if (stagingSnapshotIndex != chunk.index()) {
            if (stagingDir != null) {
                deleteRecursively(stagingDir);
            }
            stagingDir = dir.resolve(STAGING_PREFIX + chunk.index());
            Files.createDirectories(stagingDir);
            stagingSnapshotIndex = chunk.index();
        }
        atomicWrite(stagingDir.resolve(CHUNK_PREFIX + chunk.chunkNumber() + ".dat"), serialize(chunk));
    }

    @Override
    public synchronized void deleteSnapshotChunks(long snapshotIndex) throws IOException {
        deleteRecursively(dir.resolve(SNAPSHOT_PREFIX + snapshotIndex));
        if (publishedSnapshotIndex == snapshotIndex) {
            publishedSnapshotIndex = -1L;
        }
    }

    @Override
    public synchronized void flushLogs() throws IOException {
        logOut.flush();
        logChannel.force(true);

        if (stagingDir != null) {
            // fsync every chunk file, then the staging dir, then atomically
            // publish: staging-<i> -> snapshot-<i>
            try (Stream<Path> files = Files.list(stagingDir)) {
                for (Path chunkFile : files.toList()) {
                    forceFile(chunkFile);
                }
            }
            forceFile(stagingDir);
            long index = stagingSnapshotIndex;
            deleteRecursively(dir.resolve(SNAPSHOT_PREFIX + index));
            Files.move(stagingDir, dir.resolve(SNAPSHOT_PREFIX + index),
                    StandardCopyOption.ATOMIC_MOVE);
            forceFile(dir);
            stagingDir = null;
            stagingSnapshotIndex = -1L;
            publishedSnapshotIndex = index;
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (logOut != null) {
            logOut.flush();
            logOut.close();
            logOut = null;
            logFileOut = null;
            logChannel = null;
        }
    }

    @Nonnull
    @Override
    public synchronized RestoredRaftState load() throws IOException {
        if (localMember == null) {
            loadMetadata();
        }
        if (localMember == null) {
            throw new IOException("No persisted state found under " + dir);
        }

        SnapshotEntry snapshot = loadSnapshot();
        List<LogEntry> entries = readEntries();
        List<LogEntry> afterSnapshot = new ArrayList<>();
        long snapshotIndex = snapshot == null ? 0 : snapshot.index();
        for (LogEntry entry : entries) {
            if (entry.index() > snapshotIndex) {
                afterSnapshot.add(entry);
            }
        }

        return new RestoredRaftState(localMember, initialMembers, term, votedFor,
                snapshot, afterSnapshot.toArray(new LogEntry[0]));
    }

    private SnapshotEntry loadSnapshot() throws IOException {
        long newestIndex = -1L;
        Path newestDir = null;
        try (Stream<Path> entries = Files.list(dir)) {
            for (Path path : entries.toList()) {
                String name = path.getFileName().toString();
                if (name.startsWith(SNAPSHOT_PREFIX)) {
                    long index = Long.parseLong(name.substring(SNAPSHOT_PREFIX.length()));
                    if (index > newestIndex) {
                        newestIndex = index;
                        newestDir = path;
                    }
                }
            }
        }
        if (newestDir == null) {
            return null;
        }

        List<SnapshotChunk> chunks = new ArrayList<>();
        try (Stream<Path> files = Files.list(newestDir)) {
            for (Path chunkFile : files.toList()) {
                chunks.add((SnapshotChunk) deserialize(Files.readAllBytes(chunkFile)));
            }
        }
        chunks.sort(Comparator.comparingInt(SnapshotChunk::chunkNumber));

        SnapshotChunk first = chunks.get(0);
        return new SnapshotEntry(first.term(), first.index(), chunks,
                first.groupMembersLogIndex(), first.groupMembers());
    }

    private List<LogEntry> readEntries() throws IOException {
        List<byte[]> records = readRawRecords(dir.resolve(LOG_FILE));
        List<LogEntry> entries = new ArrayList<>(records.size());
        for (byte[] record : records) {
            entries.add((LogEntry) deserialize(record));
        }
        return entries;
    }

    private void writeLogFile(List<LogEntry> entries) throws IOException {
        Path logFile = dir.resolve(LOG_FILE);
        Path tmp = logFile.resolveSibling(logFile.getFileName() + TMP_SUFFIX);
        try (DataOutputStream out = new DataOutputStream(
                new BufferedOutputStream(new FileOutputStream(tmp.toFile())))) {
            for (LogEntry entry : entries) {
                byte[] record = serialize(entry);
                out.writeInt(record.length);
                out.write(record);
            }
        }
        forceFile(tmp);
        Files.move(tmp, logFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        forceFile(dir);
        reopenLog();
    }

    private void reopenLog() throws IOException {
        close();
        open();
    }

    private static List<byte[]> readRawRecords(Path file) throws IOException {
        List<byte[]> records = new ArrayList<>();
        if (!Files.exists(file)) {
            return records;
        }
        try (DataInputStream in = new DataInputStream(
                new BufferedInputStream(new FileInputStream(file.toFile())))) {
            while (true) {
                int length;
                try {
                    length = in.readInt();
                } catch (EOFException e) {
                    break; // clean end
                }
                if (length < 0 || length > MAX_RECORD_LENGTH) {
                    break; // torn/garbage tail
                }
                byte[] record = new byte[length];
                in.readFully(record); // throws EOFException on a torn tail
                records.add(record);
            }
        } catch (EOFException e) {
            // torn tail after a crash: drop the partial record
        }
        return records;
    }

    private byte[] serialize(Object object) {
        return serializationService.toData(object).toByteArray();
    }

    private Object deserialize(byte[] bytes) {
        // toObject(byte[]) is not the inverse of toData().toByteArray();
        // the payload must be wrapped back into a Data first.
        return serializationService.toObject(new HeapData(bytes));
    }

    private byte[] frame(Object first, Object second) throws IOException {
        byte[] firstBytes = serialize(first);
        byte[] secondBytes = serialize(second);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(firstBytes.length);
            out.write(firstBytes);
            out.writeInt(secondBytes.length);
            out.write(secondBytes);
        }
        return bytes.toByteArray();
    }

    private byte[] frame(int term, @Nullable RaftEndpoint votedFor) throws IOException {
        byte[] votedBytes = votedFor == null ? null : serialize(votedFor);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(term);
            out.writeBoolean(votedBytes != null);
            if (votedBytes != null) {
                out.writeInt(votedBytes.length);
                out.write(votedBytes);
            }
        }
        return bytes.toByteArray();
    }

    private void atomicWrite(Path target, byte[] bytes) throws IOException {
        Path tmp = target.resolveSibling(target.getFileName() + TMP_SUFFIX);
        try (FileOutputStream out = new FileOutputStream(tmp.toFile())) {
            out.write(bytes);
            out.getChannel().force(true);
        }
        Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        forceFile(target.getParent());
    }

    private static void forceFile(Path path) throws IOException {
        if (Files.isDirectory(path)) {
            try (FileChannel channel = FileChannel.open(path)) {
                channel.force(true);
            } catch (IOException e) {
                // directory fsync is best-effort (not supported everywhere)
            }
        } else {
            try (FileChannel channel = FileChannel.open(path)) {
                channel.force(true);
            }
        }
    }

    private static void deleteRecursively(Path path) throws IOException {
        if (!Files.exists(path)) {
            return;
        }
        if (Files.isDirectory(path)) {
            try (Stream<Path> files = Files.list(path)) {
                for (Path child : files.toList()) {
                    deleteRecursively(child);
                }
            }
        }
        Files.deleteIfExists(path);
    }
}

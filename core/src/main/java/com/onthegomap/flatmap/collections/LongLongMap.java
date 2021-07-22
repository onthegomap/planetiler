package com.onthegomap.flatmap.collections;

import static io.prometheus.client.Collector.NANOSECONDS_PER_SECOND;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongLongHashMap;
import com.graphhopper.coll.GHLongLongHashMap;
import com.graphhopper.util.StopWatch;
import com.onthegomap.flatmap.FileUtils;
import com.onthegomap.flatmap.Format;
import com.onthegomap.flatmap.MemoryEstimator;
import com.onthegomap.flatmap.monitoring.Counter;
import com.onthegomap.flatmap.monitoring.ProcessInfo;
import com.onthegomap.flatmap.monitoring.ProgressLoggers;
import com.onthegomap.flatmap.monitoring.Stats;
import com.onthegomap.flatmap.worker.Worker;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import org.mapdb.Serializer;
import org.mapdb.SortedTableMap;
import org.mapdb.volume.ByteArrayVol;
import org.mapdb.volume.MappedFileVol;
import org.mapdb.volume.Volume;
import org.rocksdb.EnvOptions;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteConfig;

public interface LongLongMap extends Closeable {

  static void main(String[] args) throws IOException, InterruptedException {
    Path path = Path.of("./llmaptest");
    FileUtils.delete(path);
    LongLongMap map = switch (args[0]) {
      case "sparsearraymemory" -> new SparseArrayMemory();
      case "hppc" -> new HppcMap();
      case "array" -> new Array();

      case "rocksdb" -> newRocksdb(path);
      case "sqlite" -> newSqlite(path);
      case "sparsearray" -> new SparseArray(path);
      case "mapdb" -> newFileBackedSortedTable(path);
      default -> throw new IllegalStateException("Unexpected value: " + args[0]);
    };
    long entries = Long.parseLong(args[1]);
    int readers = Integer.parseInt(args[2]);
    int batchSize = Integer.parseInt(args[3]);

    class LocalCounter {

      long count = 0;
    }
    LocalCounter counter = new LocalCounter();
    ProgressLoggers loggers = new ProgressLoggers("write")
      .addRatePercentCounter("entries", entries, () -> counter.count)
      .addProcessStats();
    AtomicReference<String> writeRate = new AtomicReference<>();
    new Worker("writer", new Stats.InMemory(), 1, () -> {
      long start = System.nanoTime();
      for (long i = 1; i <= entries; i++) {
        map.put(i, i + 1);
        counter.count = i;
      }
      long end = System.nanoTime();
      String rate = Format.formatNumeric(entries * NANOSECONDS_PER_SECOND / (end - start), false) + "/s";
      System.err.println("Loaded " + entries + " in " + Duration.ofNanos(end - start).toSeconds() + "s (" + rate + ")");
      writeRate.set(rate);
    }).awaitAndLog(loggers, Duration.ofSeconds(10), Duration.ofSeconds(10));

    map.get(1);
    System.err.println("Storage: " + Format.formatStorage(map.fileSize(), false));

    Counter.Readable readCount = Counter.newMultiThreadCounter();
    loggers = new ProgressLoggers("read")
      .addRateCounter("entries", readCount)
      .addProcessStats();
    CountDownLatch latch = new CountDownLatch(readers);
    for (int i = 0; i < readers; i++) {
      int rnum = i;
      new Thread(() -> {
        latch.countDown();
        try {
          latch.await();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        LongArrayList keys = new LongArrayList(batchSize);
        Random random = new Random(rnum);
        while (true) {
          keys.elementsCount = 0;
          for (int j = 0; j < batchSize; j++) {
            keys.add((Math.abs(random.nextLong()) % entries) + 1);
          }
          readCount.incBy(batchSize);
          map.multiGet(keys);
        }
      }).start();
    }
    latch.await();
    long start = System.nanoTime();
    for (int i = 0; i < 3; i++) {
      Thread.sleep(10000);
      loggers.log();
    }
    long end = System.nanoTime();
    long read = readCount.getAsLong();
    String readRate = Format.formatNumeric(read * NANOSECONDS_PER_SECOND / (end - start), false) + "/s";
    System.err.println("Read " + read + " in 30s (" + readRate + ")");
    System.err.println(
      String.join("\t",
        args[0],
        args[1],
        args[2],
        args[3],
        Format.formatStorage(ProcessInfo.getMaxMemoryBytes(), false),
        Format.formatStorage(map.fileSize(), false),
        Format.formatStorage(FileUtils.size(path), false),
        writeRate.get(),
        readRate
      )
    );
    Thread.sleep(100);
    System.exit(0);
  }

  long MISSING_VALUE = Long.MIN_VALUE;

  static LongLongMap newRocksdb(Path path) {
    return new RocksdbLongLongMap(path);
  }

  void put(long key, long value);

  long get(long key);

  long fileSize();

  default long[] multiGet(long[] key) {
    long[] result = new long[key.length];
    for (int i = 0; i < key.length; i++) {
      result[i] = get(key[i]);
    }
    return result;
  }

  default long[] multiGet(LongArrayList key) {
    long[] result = new long[key.size()];
    for (int i = 0; i < key.size(); i++) {
      result[i] = get(key.get(i));
    }
    return result;
  }

  private static Volume prepare(Path path) {
    try {
      Files.deleteIfExists(path);
    } catch (IOException e) {
      throw new IllegalStateException("Unable to delete " + path, e);
    }
    path.toFile().deleteOnExit();
    return MappedFileVol.FACTORY.makeVolume(path.toAbsolutePath().toString(), false);
  }

  private static Volume createInMemoryVolume() {
    return ByteArrayVol.FACTORY.makeVolume("", false);
  }

  static LongLongMap newFileBackedSortedTable(Path path) {
    Volume volume = prepare(path);
    return new MapdbSortedTable(volume, () -> FileUtils.size(path));
  }

  static LongLongMap newInMemorySortedTable() {
    Volume volume = createInMemoryVolume();
    return new MapdbSortedTable(volume, () -> 0);
  }

  static LongLongMap newFileBackedSparseArray(Path path) {
    return new SparseArray(path);
  }

  static LongLongMap newSqlite(Path path) {
    return new SqliteLongLongMap(path);
  }

  static LongLongMap newInMemorySparseArray() {
    return new SparseArrayMemory();
  }

  static LongLongMap newInMemorySparseArray(int segmentSize, int gapLimit) {
    return new SparseArrayMemory(segmentSize, gapLimit);
  }

  static LongLongMap newFileBackedSparseArray(Path path, int segmentSize, int gapLimit) {
    return new SparseArray(path, segmentSize, gapLimit);
  }

  static LongLongMap newArrayBacked() {
    return new Array();
  }

  static LongLongMap newInMemoryHashMap() {
    return new HppcMap();
  }

  class HppcMap implements LongLongMap {

    private final com.carrotsearch.hppc.LongLongMap underlying = new GHLongLongHashMap();

    @Override
    public void put(long key, long value) {
      underlying.put(key, value);
    }

    @Override
    public long get(long key) {
      return underlying.getOrDefault(key, MISSING_VALUE);
    }

    @Override
    public long fileSize() {
      return 0;
    }

    @Override
    public void close() throws IOException {
    }
  }

  class MapdbSortedTable implements LongLongMap {

    private final SortedTableMap.Sink<Long, Long> mapSink;
    private volatile SortedTableMap<Long, Long> map = null;
    private final LongSupplier fileSize;

    private MapdbSortedTable(Volume volume, LongSupplier fileSize) {
      mapSink = SortedTableMap.create(volume, Serializer.LONG, Serializer.LONG).createFromSink();
      this.fileSize = fileSize;
    }

    private SortedTableMap<Long, Long> getMap() {
      SortedTableMap<Long, Long> result = map;
      if (result == null) {
        synchronized (this) {
          result = map;
          if (result == null) {
            map = mapSink.create();
          }
        }
      }
      return map;
    }

    @Override
    public void put(long key, long value) {
      mapSink.put(key, value);
    }

    @Override
    public long fileSize() {
      return fileSize.getAsLong();
    }

    @Override
    public long get(long key) {
      return getMap().getOrDefault(key, MISSING_VALUE);
    }

    @Override
    public void close() {
      if (map != null) {
        map.close();
      }
    }
  }

  class Array implements LongLongMap {

    int used = 0;
    private static final long MAX_MEM_USAGE = 100_000_000_000L; // 100GB
    private static final long INDEX_OVERHEAD = 256_000_000; // 256mb
    private static final long MAX_ENTRIES = MAX_MEM_USAGE / 8L;
    private static final long MAX_SEGMENTS = INDEX_OVERHEAD / (24 + 8);
    private static final long SEGMENT_SIZE = MAX_ENTRIES / MAX_SEGMENTS + 1;

    private long[][] longs = new long[(int) MAX_SEGMENTS][];

    @Override
    public void put(long key, long value) {
      int segment = (int) (key / SEGMENT_SIZE);
      long[] seg = longs[segment];
      if (seg == null) {
        seg = longs[segment] = new long[(int) SEGMENT_SIZE];
        Arrays.fill(seg, MISSING_VALUE);
        used++;
      }
      seg[(int) (key % SEGMENT_SIZE)] = value;
    }

    @Override
    public long get(long key) {
      long[] segment = longs[(int) (key / SEGMENT_SIZE)];
      return segment == null ? MISSING_VALUE : segment[(int) (key % SEGMENT_SIZE)];
    }

    @Override
    public long fileSize() {
      return 24L + 8L * longs.length + ((long) used) * (24L + 8L * SEGMENT_SIZE);
    }

    @Override
    public void close() throws IOException {
      Arrays.fill(longs, null);
      longs = null;
    }
  }

  class SparseArray implements LongLongMap {

    private static final Logger LOGGER = LoggerFactory.getLogger(SparseArray.class);

    private static final int DEFAULT_CHUNK_SIZE = 1 << 8; // 256 (8 billion / (256mb / 8 bytes))
    private static final int DEFAULT_SEGMENT_SIZE_BYTES = 1 << 20; // 1MB
    private final long chunkSize;
    private final long segmentSize;
    private final Path path;
    private final DataOutputStream outputStream;
    private long lastKey;
    private long outIdx = 0;
    private FileChannel channel = null;
    private final LongArrayList keys = new LongArrayList();
    private volatile List<MappedByteBuffer> segments;

    SparseArray(Path path) {
      this(path, DEFAULT_SEGMENT_SIZE_BYTES, DEFAULT_CHUNK_SIZE);
    }

    public SparseArray(Path path, int segmentSize, int chunkSize) {
      this.path = path;
      this.segmentSize = segmentSize / 8;
      this.chunkSize = chunkSize;
      lastKey = -1L;
      try {
        this.outputStream = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(path), 50_000));
        appendValue(MISSING_VALUE);
      } catch (IOException e) {
        throw new IllegalStateException("Could not create compact array output stream", e);
      }
    }

    @Override
    public void put(long key, long value) {
      assert key > lastKey;
      int chunk = (int) (key / chunkSize);

      try {
        if (chunk >= keys.elementsCount) {
          while (chunk >= keys.elementsCount) {
            keys.add(outIdx);
          }
          lastKey = chunk * chunkSize;
        } else {
          lastKey++;
        }
        for (; lastKey < key; lastKey++) {
          appendValue(MISSING_VALUE);
        }
        appendValue(value);
      } catch (IOException e) {
        throw new IllegalStateException("Could not put value", e);
      }
    }

    private void appendValue(long value) throws IOException {
      outIdx++;
      outputStream.writeLong(value);
    }

    @Override
    public long get(long key) {
      if (segments == null) {
        synchronized (this) {
          if (segments == null) {
            build();
          }
        }
      }
      int chunk = (int) (key / chunkSize);
      if (key > lastKey || chunk >= keys.elementsCount) {
        return MISSING_VALUE;
      }
      long start = keys.get(chunk);
      long fileIdx = start + key % chunkSize;
      if (chunk < keys.elementsCount) {
        long next = keys.get(chunk + 1);
        if (fileIdx >= next) {
          return MISSING_VALUE;
        }
      } else {
        return MISSING_VALUE;
      }
      return getValue(fileIdx);
    }

    private void build() {
      try {
        keys.add(outIdx);
        outputStream.close();
        channel = FileChannel.open(path, StandardOpenOption.READ);
        var segmentCount = (int) (outIdx / segmentSize + 1);
        List<MappedByteBuffer> result = new ArrayList<>(segmentCount);
        LOGGER.info("LongLongMap.SparseArray gaps=" + Format.formatInteger(keys.size()) +
          " segments=" + Format.formatInteger(segmentCount));
        for (long offset = 0; offset < outIdx; offset += segmentSize) {
          result
            .add(
              channel
                .map(FileChannel.MapMode.READ_ONLY, offset << 3,
                  Math.min(segmentSize, outIdx - offset) << 3));
        }
        segments = result;
      } catch (IOException e) {
        throw new IllegalStateException("Could not create segments", e);
      }
    }

    private long getValue(long fileIdx) {
      int segNum = (int) (fileIdx / segmentSize);
      int segOffset = (int) (fileIdx % segmentSize);
      return segments.get(segNum).getLong(segOffset << 3);
    }

    private int binarySearch(long key) {
      return Arrays.binarySearch(keys.buffer, 0, keys.elementsCount, key);
    }

    @Override
    public long fileSize() {
      return FileUtils.size(path);
    }

    @Override
    public void close() throws IOException {
      outputStream.close();
      channel.close();
    }
  }

  class SparseArrayMemory implements LongLongMap {

    private static final Logger LOGGER = LoggerFactory.getLogger(SparseArrayMemory.class);

    private static final int DEFAULT_CHUNK_SIZE = 1 << 8; // 256 (8 billion / (256mb / 8 bytes))
    private static final int DEFAULT_SEGMENT_SIZE_BYTES = 1 << 20; // 1MB
    private final long chunkSize;
    private final long segmentSize;
    private long lastKey;
    private long outIdx = 0;
    private final LongArrayList keys = new LongArrayList();
    private final List<LongArrayList> segments = new ArrayList<>();

    SparseArrayMemory() {
      this(DEFAULT_SEGMENT_SIZE_BYTES, DEFAULT_CHUNK_SIZE);
    }

    public SparseArrayMemory(int segmentSize, int chunkSize) {
      this.segmentSize = segmentSize / 8;
      this.chunkSize = chunkSize;
      lastKey = -1L;
      segments.add(new LongArrayList());
      appendValue(MISSING_VALUE);
    }

    @Override
    public void put(long key, long value) {
      assert key > lastKey;
      int chunk = (int) (key / chunkSize);

      if (chunk >= keys.elementsCount) {
        while (chunk >= keys.elementsCount) {
          keys.add(outIdx);
        }
        lastKey = chunk * chunkSize;
      } else {
        lastKey++;
      }
      for (; lastKey < key; lastKey++) {
        appendValue(MISSING_VALUE);
      }
      appendValue(value);
    }

    private void appendValue(long value) {
      outIdx++;
      var last = segments.get(segments.size() - 1);
      if (last.size() >= segmentSize) {
        segments.add(last = new LongArrayList());
      }
      last.add(value);
    }

    private volatile boolean init = false;

    @Override
    public long get(long key) {
      if (!init) {
        synchronized (this) {
          if (!init) {
            keys.add(outIdx);
            init = true;
          }
        }
      }
      int chunk = (int) (key / chunkSize);
      if (key > lastKey || chunk >= keys.elementsCount) {
        return MISSING_VALUE;
      }
      long start = keys.get(chunk);
      long fileIdx = start + key % chunkSize;
      if (chunk < keys.elementsCount) {
        long next = keys.get(chunk + 1);
        if (fileIdx >= next) {
          return MISSING_VALUE;
        }
      } else {
        return MISSING_VALUE;
      }
      return getValue(fileIdx);
    }

    private long getValue(long fileIdx) {
      int segNum = (int) (fileIdx / segmentSize);
      int segOffset = (int) (fileIdx % segmentSize);
      return segments.get(segNum).get(segOffset);
    }

    @Override
    public long fileSize() {
      return MemoryEstimator.size(keys) + segments.stream().mapToLong(MemoryEstimator::size).sum();
    }

    @Override
    public void close() throws IOException {
      keys.release();
      segments.forEach(LongArrayList::release);
      segments.clear();
    }
  }


  class SqliteLongLongMap implements LongLongMap {

    static {
      try {
        Class.forName("org.sqlite.JDBC");
      } catch (ClassNotFoundException e) {
        throw new IllegalStateException("JDBC driver not found");
      }
    }

    private final static int batchSize = 499;
    private Path path = null;
    private Connection conn;
    private final PreparedStatement batchInsert;
    private final long[] batch = new long[batchSize * 2];
    private int inBatch = 0;

    SqliteLongLongMap(Path path) {
      this.path = path;
      try {
        SQLiteConfig config = new SQLiteConfig();
        config.setSynchronous(SQLiteConfig.SynchronousMode.OFF);
        config.setJournalMode(SQLiteConfig.JournalMode.OFF);
        config.setLockingMode(SQLiteConfig.LockingMode.EXCLUSIVE);
        config.setTempStore(SQLiteConfig.TempStore.MEMORY);
        config.setPageSize(8_192);
        config.setPragma(SQLiteConfig.Pragma.MMAP_SIZE, "30000000000");

        conn = DriverManager.getConnection("jdbc:sqlite:" + path.toAbsolutePath(), config.toProperties());
        execute("drop table if exists kv;");
        execute("create table kv (key INTEGER not null primary key asc, value integer not null);");
        StringBuilder statement = new StringBuilder("REPLACE INTO kv (key, value) values ");
        for (int i = 0; i < batchSize; i++) {
          statement.append("(?,?), ");
        }
        batchInsert = conn.prepareStatement(statement.toString().replaceAll("..$", ";"));
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }

    private SqliteLongLongMap execute(String... queries) {
      for (String query : queries) {
        try (var statement = conn.createStatement()) {
          statement.execute(query);
        } catch (SQLException throwables) {
          throw new IllegalStateException("Error executing queries " + Arrays.toString(queries), throwables);
        }
      }
      return this;
    }

    private volatile boolean readable = false;

    public void makeReadable() {
      if (!readable) {
        synchronized (this) {
          if (!readable) {
            try {
              System.err.println("Making readable");
              flush();
              conn.close();
            } catch (SQLException e) {
              throw new RuntimeException(e);
            }
            readable = true;
          }
        }
      }
    }

    private static record PerThreadConnection(
      Connection conn, PreparedStatement select, Map<Integer, PreparedStatement> selects
    ) {

      private PreparedStatement selectFor(int num) throws SQLException {
        if (selects.containsKey(num)) {
          return selects.get(num);
        } else {
          StringBuilder builder = new StringBuilder("SELECT key, value from kv where key in (");
          for (int i = 0; i < num; i++) {
            builder.append("?,");
          }
          builder.append(")");
          PreparedStatement stmt = conn.prepareStatement(builder.toString().replaceAll(",\\)$", ");"));
          selects.put(num, stmt);
          return selects.get(num);
        }
      }
    }

    private final List<PerThreadConnection> conns = new ArrayList<>();
    private final ThreadLocal<PerThreadConnection> threadConn = ThreadLocal.withInitial(() -> {
      SQLiteConfig config = new SQLiteConfig();
      config.setReadOnly(true);
      config.setCacheSize(100_000);
      config.setLockingMode(SQLiteConfig.LockingMode.EXCLUSIVE);
      config.setPageSize(32_768);
      try {
        Connection thisConn = DriverManager
          .getConnection("jdbc:sqlite:" + path.toAbsolutePath(), config.toProperties());
        PerThreadConnection result = new PerThreadConnection(
          thisConn,
          thisConn.prepareStatement("select value from kv where key = ?;"),
          new HashMap<>()
        );
        conns.add(result);
        return result;
      } catch (SQLException throwables) {
        throw new IllegalStateException(throwables);
      }
    });

    @Override
    public void put(long key, long val) {
      batch[inBatch * 2] = key;
      batch[inBatch * 2 + 1] = val;
      inBatch++;
      if (inBatch >= batchSize) {
        flush();
      }
    }

    @Override
    public long get(long key) {
      makeReadable();
      try {
        PreparedStatement select = threadConn.get().select;
        select.setLong(1, key);
        try (ResultSet set = select.executeQuery()) {
          if (set.next()) {
            return set.getLong(1);
          } else {
            return Long.MIN_VALUE;
          }
        }
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public long fileSize() {
      return FileUtils.fileSize(path);
    }

    @Override
    public long[] multiGet(LongArrayList key) {
      return multiGet(key.toArray());
    }

    @Override
    public long[] multiGet(long[] key) {
      try {
        makeReadable();
        long[] result = new long[key.length];
        Arrays.fill(result, MISSING_VALUE);
        PerThreadConnection conn = threadConn.get();
        LongLongHashMap mapping = new LongLongHashMap(key.length);
        for (int i = 0; i < key.length; i += 100) {
          int size = Math.min(100, key.length - i);
          PreparedStatement select = conn.selectFor(size);
          for (int j = 0; j < size; j++) {
            select.setLong(j + 1, key[j + i]);
          }
          try (ResultSet set = select.executeQuery()) {
            while (set.next()) {
              long k = set.getLong(1);
              long v = set.getLong(2);
              mapping.put(k, v);
            }
          }
        }
        for (int i = 0; i < key.length; i++) {
          result[i] = mapping.getOrDefault(key[i], MISSING_VALUE);
        }
        return result;
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void close() {
      flush();
      try {
        conn.close();
        for (PerThreadConnection conn2 : conns) {
          conn2.conn.close();
        }
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }

    private void flush() {
      if (inBatch == batchSize) {
        try {
          for (int i = 0; i < batch.length; i++) {
            batchInsert.setLong(i + 1, batch[i]);
          }
          batchInsert.execute();
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      } else if (inBatch > 0) {
        for (int i = 0; i < inBatch; i++) {
          execute("REPLACE into kv (key, value) values (" + batch[i * 2] + "," + batch[i * 2 + 1] + ");");
        }
      }
      inBatch = 0;
    }
  }

  private static byte[] encodeNodeId(long id) {
    return new byte[]{
      (byte) (id >> 32),
      (byte) (id >> 24),
      (byte) (id >> 16),
      (byte) (id >> 8),
      (byte) id,
    };
  }

  private static byte[] encodeNodeValue(long id) {
    return new byte[]{
      (byte) (id >> 56),
      (byte) (id >> 48),
      (byte) (id >> 40),
      (byte) (id >> 32),
      (byte) (id >> 24),
      (byte) (id >> 16),
      (byte) (id >> 8),
      (byte) id,
    };
  }

  private static long decodeNodeValue(byte[] b) {
    return ((long) b[0] << 56)
      | ((long) b[1] & 0xff) << 48
      | ((long) b[2] & 0xff) << 40
      | ((long) b[3] & 0xff) << 32
      | ((long) b[4] & 0xff) << 24
      | ((long) b[5] & 0xff) << 16
      | ((long) b[6] & 0xff) << 8
      | ((long) b[7] & 0xff);
  }

  class RocksdbLongLongMap implements LongLongMap {

    private final Path sst;
    private final SstFileWriter writer;

    static {
      RocksDB.loadLibrary();
    }

    private Path path = null;

    public RocksdbLongLongMap(Path path) {
      this.path = path;
      try {
        FileUtils.delete(path);
        Files.createDirectories(path);
        writer = new SstFileWriter(new EnvOptions(), new Options());
        FileUtils.delete(Path.of("/tmp/rocks"));
        Files.createDirectories(Path.of("/tmp/rocks"));
        sst = Path.of("/tmp/rocks").resolve("sst");
        writer.open(sst.toAbsolutePath().toString());
      } catch (RocksDBException | IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    private volatile boolean compacted = false;

    private ThreadLocal<RocksDB> rocksDb = ThreadLocal.withInitial(() -> {
      try {
        return RocksDB.openReadOnly(new Options()
            .setAllowMmapReads(true)
          , path.resolve("db").toAbsolutePath().toString());
      } catch (RocksDBException e) {
        return null;
      }
    });

    private synchronized void compactIfNecessary() {
      if (!compacted) {
        synchronized (this) {
          if (!compacted) {
            try {
              System.err.println("Ingesting...");
              StopWatch watch = new StopWatch().start();
              writer.finish();
              writer.close();
              try (RocksDB _db = RocksDB.open(new Options().setCreateIfMissing(true),
                path.resolve("db").toAbsolutePath().toString())) {
                _db.ingestExternalFile(List.of(sst.toAbsolutePath().toString()), new IngestExternalFileOptions());
              } finally {
                FileUtils.delete(sst);
              }

              System.err.println("Done. Took " + (watch.stop().getCurrentSeconds()) + "s");
            } catch (RocksDBException e) {
              throw new Error(e);
            }
            compacted = true;
          }
        }
      }
    }

    @Override
    public void put(long key, long val) {
      try {
        writer.put(LongLongMap.encodeNodeId(key), LongLongMap.encodeNodeValue(val));
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public long get(long key) {
      compactIfNecessary();
      try {
        byte[] results = rocksDb.get().get(LongLongMap.encodeNodeId(key));
        return results == null ? MISSING_VALUE : LongLongMap.decodeNodeValue(results);
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public long[] multiGet(LongArrayList key) {
      compactIfNecessary();
      long[] result = new long[key.size()];
      try {
        List<byte[]> keys = new ArrayList<>(key.size());
        for (int i = 0; i < key.size(); i++) {
          keys.add(LongLongMap.encodeNodeId(key.get(i)));
        }
        List<byte[]> results = rocksDb.get().multiGetAsList(keys);
        for (int i = 0; i < results.size(); i++) {
          byte[] thisResult = results.get(i);
          result[i] = thisResult == null ? MISSING_VALUE : LongLongMap.decodeNodeValue(thisResult);
        }
        return result;
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public long[] multiGet(long[] key) {
      return multiGet(LongArrayList.from(key));
    }

    @Override
    public long fileSize() {
      return FileUtils.size(path);
    }

    @Override
    public void close() {
//      db.close();
    }
  }
}

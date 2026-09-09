package common.cn.kafei.simukraft.storage;

import common.cn.kafei.simukraft.SimuKraft;
import common.cn.kafei.simukraft.storage.core.SqlFunction;
import common.cn.kafei.simukraft.storage.core.SqlWrite;
import common.cn.kafei.simukraft.storage.core.SqliteConnectionPool;
import common.cn.kafei.simukraft.storage.core.StorageMetrics;
import common.cn.kafei.simukraft.storage.core.TransactionRunner;
import net.minecraft.server.MinecraftServer;
import net.minecraft.world.level.storage.LevelResource;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 已建成建筑结构库。
 * <p>与主库共用同一套引擎：{@link SqliteConnectionPool} 管连接、{@link TransactionRunner} 管事务、
 * 独立 {@link StorageWriteQueue}（写线程名 simukraft-buildings-db-write）管写入顺序。
 * 建筑完工/拆除的写入经 {@link #callSync} 提交到写线程执行并同步等待结果，
 * 调用方拿到真实落库结果，主线程不再每次操作新建 JDBC 连接并重设 PRAGMA。
 * <p>实例按 {@link MinecraftServer} 缓存，关服时 {@link #closeFor} 释放，避免跨存档复用；
 * 关服后 {@link #open} 返回 null，不让迟到的调用重建出一个没人关闭的僵尸实例。
 */

public final class BuildingStructureSqliteDatabase implements Closeable {
    private static final String STORAGE_DIR = SimuKraft.MOD_ID;
    private static final String DATABASE_FILE = SimuKraft.MOD_ID + "_buildings.sqlite";
    private static final long SYNC_WRITE_TIMEOUT_MILLIS = 30_000L;
    private static final ConcurrentMap<MinecraftServer, BuildingStructureSqliteDatabase> INSTANCES = new ConcurrentHashMap<>();
    // SHUTDOWN 阻止已关闭的服务器被残留调用重新注册出一个新实例（新连接池 + 新写线程 + 重跑 schema）。
    private static final Set<MinecraftServer> SHUTDOWN = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private final Path databasePath;
    private final SqliteConnectionPool connections;
    private final TransactionRunner transactions;
    private final StorageMetrics metrics = new StorageMetrics();
    private final StorageWriteQueue writeQueue;
    // degraded：写入出现环境故障后置位，此后写入被拒绝，语义与主库一致。
    private volatile boolean degraded;
    private volatile boolean closed;

    private BuildingStructureSqliteDatabase(Path databasePath) {
        this.databasePath = databasePath;
        createStorageDirectory(databasePath);
        this.connections = SqliteConnectionPool.open(databasePath);
        this.transactions = new TransactionRunner(connections, this::markDegraded, metrics);
        try {
            BuildingStructureSqliteSchema.initialize(connections);
        } catch (RuntimeException exception) {
            connections.close();
            throw exception;
        }
        this.writeQueue = new StorageWriteQueue("simukraft-buildings-db-write", transactions, metrics);
    }

    /** open: 取该存档的建筑库实例；服务器已关服、为 null 或建库失败时返回 null，调用方按"存储不可用"处理。 */
    public static BuildingStructureSqliteDatabase open(MinecraftServer server) {
        if (server == null || SHUTDOWN.contains(server)) {
            return null;
        }
        try {
            return INSTANCES.computeIfAbsent(server, key -> {
                Path worldPath = key.getWorldPath(LevelResource.ROOT).toAbsolutePath().normalize();
                return new BuildingStructureSqliteDatabase(worldPath.resolve(STORAGE_DIR).resolve(DATABASE_FILE));
            });
        } catch (RuntimeException exception) {
            // 与主库 openSafely 同一防线：建库失败退回"仅内存"，不让每 tick 的服务调用跟着炸。
            SimuKraft.LOGGER.error("Building structure SQLite storage is unavailable. Placed buildings will run on in-memory state only.", exception);
            return null;
        }
    }

    /** closeFor: 关服时释放该存档的实例，避免跨存档复用；之后 {@link #open} 不再重建。 */
    public static void closeFor(MinecraftServer server) {
        if (server == null) {
            return;
        }
        SHUTDOWN.add(server);
        BuildingStructureSqliteDatabase database = INSTANCES.remove(server);
        if (database != null) {
            database.close();
        }
    }

    /** forgetServer: 服务器实例彻底退出后释放引用，避免 SHUTDOWN 集合长期持有强引用。 */
    public static void forgetServer(MinecraftServer server) {
        if (server != null) {
            INSTANCES.remove(server);
            SHUTDOWN.remove(server);
        }
    }

    /** borrowConnection: 借一条池化连接用于查询，调用方必须 close（try-with-resources）归还。 */
    public Connection borrowConnection() throws SQLException {
        return connections.borrow();
    }

    /** callSync: 把写入提交到写线程执行并阻塞等待结果；失败、已关闭或已降级返回 null。 */
    public <T> T callSync(SqlFunction<T> function) {
        if (isWriteBlocked()) {
            return null;
        }
        return writeQueue.submitAndWait(SYNC_WRITE_TIMEOUT_MILLIS, function);
    }

    /** submitAsync: 把写入提交到写队列立即返回，不阻塞调用线程。写入失败时写线程已记录日志。 */
    public void submitAsync(Object key, SqlWrite write) {
        if (isWriteBlocked()) {
            SimuKraft.LOGGER.warn("Simukraft: async write for key {} skipped, storage is write-blocked.", key);
            return;
        }
        writeQueue.submit(key, write);
    }

    /**
     * isWriteBlocked: 写入通道是否整体不可用（已关闭或已降级）。
     * <p>与"单次写入失败"必须区分开：不可用是整会话粘性的，调用方据此走"仅内存"降级路径，
     * 而不是把每一次业务操作都当成失败作废。
     */
    public boolean isWriteBlocked() {
        return closed || degraded;
    }

    /** markDegraded: 记录一次环境故障，之后写入被拒绝直到重开存档。 */
    public void markDegraded(String context, Throwable cause) {
        if (!degraded) {
            degraded = true;
            SimuKraft.LOGGER.error("Simukraft: building structure SQLite storage entered DEGRADED mode ({}). Writes are disabled to protect existing data.", context, cause);
        }
    }

    public boolean isDegraded() {
        return degraded;
    }

    public boolean isClosed() {
        return closed;
    }

    /** drainWrites: 等待队列中的写入全部落库，关服与测试用。 */
    public boolean drainWrites() {
        return writeQueue.drainAndReport();
    }

    /** pendingWrites: 当前仍在队列中等待落库的写入条数（指标用）。 */
    public int pendingWrites() {
        return writeQueue.pendingCount();
    }

    public StorageMetrics metrics() {
        return metrics;
    }

    /** summarizeFor: 输出该存档建筑库的指标快照；实例不存在时返回未初始化说明。 */
    public static String summarizeFor(MinecraftServer server) {
        BuildingStructureSqliteDatabase database = server != null ? INSTANCES.get(server) : null;
        return database != null
                ? database.metrics.summarize(database.pendingWrites(), database.degraded)
                : "not-initialized";
    }

    public Path databasePath() {
        return databasePath;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        writeQueue.drainAndReport();
        writeQueue.close(5_000L);
        connections.close();
    }

    private static void createStorageDirectory(Path databasePath) {
        try {
            Files.createDirectories(databasePath.getParent());
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to create building structure SQLite directory", exception);
        }
    }
}

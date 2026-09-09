package common.cn.kafei.simukraft.storage.core;

import common.cn.kafei.simukraft.SimuKraft;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * 在常驻写连接上执行事务，负责提交、回滚、SQLITE_BUSY 退避重试与失败分类。
 *
 * <p>一批写入合并进一个事务，把每次写入一次 fsync 降成每批一次。
 * 整批失败时会逐条重试，避免一条坏数据把同批其它写入一起拖掉。
 * 彻底失败按 {@link StorageErrorPolicy} 分类：操作自身问题丢弃该条继续，
 * 环境故障上报降级处理器。
 */
public final class TransactionRunner {
    private static final int MAX_ATTEMPTS = 4;
    private static final long INITIAL_BACKOFF_MILLIS = 20L;
    // SQLITE_BUSY = 5, SQLITE_LOCKED = 6；两者都表示"稍后重试可能成功"。
    private static final int SQLITE_BUSY = 5;
    private static final int SQLITE_LOCKED = 6;

    private final SqliteConnectionPool connections;
    private final StorageErrorPolicy.FaultHandler faultHandler;
    private final StorageMetrics metrics;

    public TransactionRunner(SqliteConnectionPool connections, StorageErrorPolicy.FaultHandler faultHandler, StorageMetrics metrics) {
        this.connections = connections;
        this.faultHandler = faultHandler;
        this.metrics = metrics;
    }

    /**
     * runBatch: 把一批写入放进一个事务执行。
     *
     * @return 落库失败的写入条数
     */
    public int runBatch(List<SqlWrite> batch) {
        if (batch.isEmpty()) {
            return 0;
        }
        // 整批尝试：成功则各 op 在 commit 后收到通知；失败先不通知，等逐条重试出最终结果再逐个通知。
        if (tryInTransaction(batch, false)) {
            return 0;
        }
        // 整批失败：逐条重试，定位并隔离真正出错的那条。
        int failed = 0;
        for (SqlWrite operation : batch) {
            if (!tryInTransaction(List.of(operation), true)) {
                failed++;
                metrics.recordFailed(1);
            }
        }
        if (failed > 0) {
            SimuKraft.LOGGER.error("Simukraft: {} of {} storage write(s) in this batch could not be persisted.", failed, batch.size());
        }
        return failed;
    }

    /** runSingle: 在常驻写连接上执行一次独立事务。 */
    public boolean runSingle(SqlWrite operation) {
        boolean succeeded = tryInTransaction(List.of(operation), true);
        if (!succeeded) {
            metrics.recordFailed(1);
        }
        return succeeded;
    }

    private boolean tryInTransaction(List<SqlWrite> operations, boolean notifyFailure) {
        long backoffMillis = INITIAL_BACKOFF_MILLIS;
        long startedAt = System.nanoTime();
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            Connection connection = null;
            try {
                connection = connections.writeConnection();
                connection.setAutoCommit(false);
                for (SqlWrite operation : operations) {
                    operation.write(connection);
                }
                connection.commit();
                notifyAfterCommit(operations, true);
                long elapsedMillis = (System.nanoTime() - startedAt) / 1_000_000L;
                metrics.recordBatch(operations.size(), elapsedMillis);
                if (elapsedMillis >= StorageMetrics.SLOW_BATCH_MILLIS) {
                    SimuKraft.LOGGER.warn("Simukraft: slow storage batch: {} op(s) took {} ms", operations.size(), elapsedMillis);
                }
                return true;
            } catch (SQLException exception) {
                rollbackQuietly(connection);
                if (!isRetryable(exception) || attempt == MAX_ATTEMPTS) {
                    SimuKraft.LOGGER.error("Simukraft: storage transaction failed after {} attempt(s)", attempt, exception);
                    // 只在最终结果确定（逐条隔离重试也失败）时才上报降级；整批尝试失败会先重试，不重复上报。
                    if (notifyFailure) {
                        reportIfEnvironmentFault(exception);
                        notifyAfterCommit(operations, false);
                    }
                    return false;
                }
                sleep(backoffMillis);
                backoffMillis *= 2;
            } catch (RuntimeException exception) {
                rollbackQuietly(connection);
                // RuntimeException 一律是操作自身的代码/数据问题（OP_FAULT），不触发降级。
                SimuKraft.LOGGER.error("Simukraft: storage transaction aborted by an unexpected error", exception);
                if (notifyFailure) {
                    notifyAfterCommit(operations, false);
                }
                return false;
            } finally {
                releaseWriteTransaction(connection);
                connections.checkpointPassive();
            }
        }
        return false;
    }

    /** reportIfEnvironmentFault: 环境故障（IO/磁盘满/损坏/忙超时耗尽）上报降级处理器。 */
    private void reportIfEnvironmentFault(SQLException exception) {
        if (StorageErrorPolicy.classify(exception) == StorageErrorPolicy.StorageFault.ENV_FAULT) {
            faultHandler.onEnvironmentFault("storage transaction", exception);
        }
    }

    /** notifyAfterCommit: 回调关心事务最终结果的写入；回调异常不影响其它写入。 */
    private static void notifyAfterCommit(List<SqlWrite> operations, boolean committed) {
        for (SqlWrite operation : operations) {
            if (operation instanceof CommitAwareWrite aware) {
                try {
                    aware.afterCommit(committed);
                } catch (RuntimeException exception) {
                    SimuKraft.LOGGER.warn("Simukraft: post-commit callback failed", exception);
                }
            }
        }
    }

    private static boolean isRetryable(SQLException exception) {
        for (Throwable cause = exception; cause != null; cause = cause.getCause()) {
            if (cause instanceof SQLException sqlException) {
                int code = sqlException.getErrorCode();
                // sqlite-jdbc 会把主错误码和扩展错误码都放在 errorCode 里，扩展码低 8 位是主码。
                if (code == SQLITE_BUSY || code == SQLITE_LOCKED
                        || (code & 0xFF) == SQLITE_BUSY || (code & 0xFF) == SQLITE_LOCKED) {
                    return true;
                }
            }
        }
        return false;
    }

    /** releaseWriteTransaction: 批结束后回到 autoCommit，避免 sqlite-jdbc 在 commit 后再 BEGIN 钉住 WAL。 */
    private static void releaseWriteTransaction(Connection connection) {
        if (connection == null) {
            return;
        }
        try {
            if (!connection.isClosed() && !connection.getAutoCommit()) {
                connection.setAutoCommit(true);
            }
        } catch (SQLException exception) {
            SimuKraft.LOGGER.warn("Simukraft: failed to release SQLite write transaction", exception);
        }
    }

    private static void rollbackQuietly(Connection connection) {
        if (connection == null) {
            return;
        }
        try {
            if (!connection.getAutoCommit()) {
                connection.rollback();
            }
        } catch (SQLException exception) {
            SimuKraft.LOGGER.warn("Simukraft: failed to roll back storage transaction", exception);
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }
}

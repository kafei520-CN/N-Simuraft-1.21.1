package common.cn.kafei.simukraft.storage.core;

import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** 锁定失败分类语义：约束冲突丢弃该操作继续，环境故障才触发降级。 */
class StorageErrorPolicyTest {

    @Test
    void constraintViolationsAreOperationFaults() {
        assertEquals(StorageErrorPolicy.StorageFault.OP_FAULT,
                StorageErrorPolicy.classify(new SQLException("constraint", "23000", 19)));
        // SQLITE_CONSTRAINT_PRIMARYKEY = 19 | (6 << 8) = 1555，扩展码低 8 位是主码。
        assertEquals(StorageErrorPolicy.StorageFault.OP_FAULT,
                StorageErrorPolicy.classify(new SQLException("pk", "23000", 1555)));
    }

    @Test
    void environmentProblemsAreEnvironmentFaults() {
        assertEquals(StorageErrorPolicy.StorageFault.ENV_FAULT,
                StorageErrorPolicy.classify(new SQLException("cantopen", "HY000", 14)));
        assertEquals(StorageErrorPolicy.StorageFault.ENV_FAULT,
                StorageErrorPolicy.classify(new SQLException("ioerr", "HY000", 10)));
        // 忙/锁重试耗尽后仍是环境故障。
        assertEquals(StorageErrorPolicy.StorageFault.ENV_FAULT,
                StorageErrorPolicy.classify(new SQLException("busy", "HY000", 5)));
        assertEquals(StorageErrorPolicy.StorageFault.ENV_FAULT,
                StorageErrorPolicy.classify(new SQLException("database or disk is full", "HY000", 13)));
        // 未知错误一律按环境处理，宁可保守。
        assertEquals(StorageErrorPolicy.StorageFault.ENV_FAULT,
                StorageErrorPolicy.classify(new SQLException("unknown")));
    }

    @Test
    void runtimeExceptionsAreOperationFaults() {
        assertEquals(StorageErrorPolicy.StorageFault.OP_FAULT,
                StorageErrorPolicy.classify(new IllegalStateException("boom")));
    }
}

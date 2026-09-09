package common.cn.kafei.simukraft.storage;

import common.cn.kafei.simukraft.SimuKraft;
import common.cn.kafei.simukraft.building.BuildingBlockData;
import common.cn.kafei.simukraft.storage.core.SqliteConnectionPool;
import net.minecraft.core.BlockPos;
import net.minecraft.world.level.block.state.BlockState;

import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * 建筑库 v2：把 placed_building_blocks 按栋折成 blocks_payload。
 * <p>按栋独立提交，中断后下次开库从 blocks_format=0 的栋接着转，避免整库一把事务在盘满时滚回去。
 */
final class BuildingStructureMigrations {
    static final int VERSION = 2;

    private BuildingStructureMigrations() {
    }

    /** upgradeToV2: 给旧档补列、按栋折叠、删掉方块表。已经是 v2 则立即返回。 */
    static void upgradeToV2(SqliteConnectionPool connections) throws SQLException {
        connections.checkpoint();
        try (Connection connection = connections.borrow()) {
            if (readUserVersion(connection) >= VERSION) {
                return;
            }
            addPayloadColumns(connection);
            foldAllBuildings(connection, connections);
            dropLegacyBlockTable(connection);
            writeUserVersion(connection, VERSION);
        }
        vacuumIfSpaceAllows(connections);
        SimuKraft.LOGGER.info("Simukraft: building structure SQLite schema is now at user_version={}.", VERSION);
    }

    private static void addPayloadColumns(Connection connection) throws SQLException {
        addColumnIfMissing(connection, "placed_buildings", "blocks_format", "INTEGER NOT NULL DEFAULT 0");
        addColumnIfMissing(connection, "placed_buildings", "blocks_payload", "BLOB");
        addColumnIfMissing(connection, "placed_buildings", "block_count", "INTEGER NOT NULL DEFAULT 0");
    }

    private static void foldAllBuildings(Connection connection, SqliteConnectionPool connections) throws SQLException {
        if (!tableExists(connection, "placed_building_blocks")) {
            markRemainingBuildingsConverted(connection);
            return;
        }
        List<String> pendingIds = listPendingBuildingIds(connection);
        SimuKraft.LOGGER.info("Simukraft: folding {} placed building(s) into voxel snapshots.", pendingIds.size());
        for (String buildingId : pendingIds) {
            foldOneBuilding(connection, buildingId);
            connections.checkpointPassive();
        }
        markRemainingBuildingsConverted(connection);
    }

    private static List<String> listPendingBuildingIds(Connection connection) throws SQLException {
        List<String> ids = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT building_id FROM placed_buildings WHERE blocks_format = 0 ORDER BY building_id");
             ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                ids.add(resultSet.getString(1));
            }
        }
        return ids;
    }

    private static void foldOneBuilding(Connection connection, String buildingId) throws SQLException {
        boolean previousAutoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);
        try {
            List<BuildingBlockData> blocks = loadLegacyBlocks(connection, buildingId);
            byte[] payload = BuildingVoxelCodec.encode(blocks);
            try (PreparedStatement update = connection.prepareStatement(
                    "UPDATE placed_buildings SET blocks_format = ?, blocks_payload = ?, block_count = ? WHERE building_id = ?")) {
                update.setInt(1, BuildingVoxelCodec.FORMAT_V1);
                update.setBytes(2, payload);
                update.setInt(3, blocks.size());
                update.setString(4, buildingId);
                update.executeUpdate();
            }
            try (PreparedStatement delete = connection.prepareStatement(
                    "DELETE FROM placed_building_blocks WHERE building_id = ?")) {
                delete.setString(1, buildingId);
                delete.executeUpdate();
            }
            connection.commit();
        } catch (SQLException exception) {
            try {
                connection.rollback();
            } catch (SQLException rollbackFailure) {
                exception.addSuppressed(rollbackFailure);
            }
            throw exception;
        } finally {
            connection.setAutoCommit(previousAutoCommit);
        }
    }

    private static List<BuildingBlockData> loadLegacyBlocks(Connection connection, String buildingId) throws SQLException {
        List<BuildingBlockData> blocks = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT relative_x, relative_y, relative_z, block_id, block_state_nbt, original_x, original_y, original_z "
                        + "FROM placed_building_blocks WHERE building_id = ?")) {
            statement.setString(1, buildingId);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    String blockId = resultSet.getString("block_id");
                    BlockState state = BuildingVoxelCodec.decodeLegacyBlockState(resultSet.getString("block_state_nbt"), blockId);
                    if (state == null || state.isAir()) {
                        continue;
                    }
                    blocks.add(new BuildingBlockData(
                            new BlockPos(resultSet.getInt("relative_x"), resultSet.getInt("relative_y"), resultSet.getInt("relative_z")),
                            state,
                            new BlockPos(resultSet.getInt("original_x"), resultSet.getInt("original_y"), resultSet.getInt("original_z"))
                    ));
                }
            }
        }
        return blocks;
    }

    /** markRemainingBuildingsConverted: 没有旧方块行的建筑也标成已折叠，避免下次再扫。 */
    private static void markRemainingBuildingsConverted(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("UPDATE placed_buildings SET blocks_format = " + BuildingVoxelCodec.FORMAT_V1
                    + " WHERE blocks_format = 0");
        }
    }

    private static void dropLegacyBlockTable(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("DROP TABLE IF EXISTS placed_building_blocks");
        }
    }

    private static void vacuumIfSpaceAllows(SqliteConnectionPool connections) {
        try {
            var path = connections.databasePath();
            long size = Files.size(path);
            long free = Files.getFileStore(path).getUsableSpace();
            if (free < size) {
                SimuKraft.LOGGER.info("Simukraft: skipping building SQLite VACUUM because free space {} < database size {}.", free, size);
                return;
            }
            connections.checkpoint();
            try (Connection connection = connections.borrow(); Statement statement = connection.createStatement()) {
                statement.execute("VACUUM");
            }
        } catch (IOException | SQLException exception) {
            SimuKraft.LOGGER.warn("Simukraft: building SQLite VACUUM skipped", exception);
        }
    }

    private static void addColumnIfMissing(Connection connection, String tableName, String columnName, String columnDefinition) throws SQLException {
        if (hasColumn(connection, tableName, columnName)) {
            return;
        }
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("ALTER TABLE " + tableName + " ADD COLUMN " + columnName + " " + columnDefinition);
        }
    }

    private static boolean hasColumn(Connection connection, String tableName, String columnName) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("PRAGMA table_info(" + tableName + ")")) {
            while (resultSet.next()) {
                if (columnName.equalsIgnoreCase(resultSet.getString("name"))) {
                    return true;
                }
            }
            return false;
        }
    }

    private static boolean tableExists(Connection connection, String tableName) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?")) {
            statement.setString(1, tableName);
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next();
            }
        }
    }

    private static int readUserVersion(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("PRAGMA user_version")) {
            return resultSet.next() ? resultSet.getInt(1) : 0;
        }
    }

    private static void writeUserVersion(Connection connection, int version) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("PRAGMA user_version = " + version);
        }
    }
}

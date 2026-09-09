package common.cn.kafei.simukraft.storage;

import common.cn.kafei.simukraft.SimuKraft;
import common.cn.kafei.simukraft.storage.core.SqliteConnectionPool;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * 建筑结构库 schema。
 * <p>v1：三张表，方块一行一格。v2：方块折进 {@code blocks_payload}，不再建 {@code placed_building_blocks}。
 */
public final class BuildingStructureSqliteSchema {
    private static final DateTimeFormatter BACKUP_STAMP = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");
    static final int CURRENT_VERSION = BuildingStructureMigrations.VERSION;

    private BuildingStructureSqliteSchema() {
    }

    public static void initialize(SqliteConnectionPool connections) {
        try (Connection connection = connections.borrow()) {
            int version = readUserVersion(connection);
            if (version >= CURRENT_VERSION) {
                return;
            }
            if (version == 0 && !tableExists(connection, "placed_buildings")) {
                createV2Schema(connection);
                writeUserVersion(connection, CURRENT_VERSION);
                SimuKraft.LOGGER.info("Simukraft: building structure SQLite schema baseline applied (user_version={}).", CURRENT_VERSION);
                return;
            }
            if (version == 0) {
                createV1Baseline(connection);
                writeUserVersion(connection, 1);
                version = 1;
                SimuKraft.LOGGER.info("Simukraft: building structure SQLite schema baseline applied (user_version=1).");
            }
            if (version == 1) {
                connections.checkpoint();
                backup(connections.databasePath(), version);
            }
        } catch (SQLException exception) {
            throw new IllegalStateException("Failed to initialize building structure SQLite database", exception);
        }
        try {
            BuildingStructureMigrations.upgradeToV2(connections);
        } catch (SQLException exception) {
            throw new IllegalStateException("Failed to migrate building structure SQLite database to v2", exception);
        }
    }

    /** createV2Schema: 全新档直接建带 payload 的目录表，不创建方块行表。 */
    private static void createV2Schema(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("CREATE TABLE IF NOT EXISTS placed_buildings("
                    + "building_id TEXT PRIMARY KEY, city_id TEXT, dimension_id TEXT NOT NULL, category TEXT NOT NULL, "
                    + "building_file_name TEXT NOT NULL, display_name TEXT NOT NULL, amount TEXT NOT NULL DEFAULT '', "
                    + "structure_file_name TEXT NOT NULL, facing TEXT NOT NULL, origin_x INTEGER NOT NULL, origin_y INTEGER NOT NULL, "
                    + "origin_z INTEGER NOT NULL, anchor_x INTEGER NOT NULL, anchor_y INTEGER NOT NULL, anchor_z INTEGER NOT NULL, "
                    + "min_x INTEGER NOT NULL, min_y INTEGER NOT NULL, min_z INTEGER NOT NULL, max_x INTEGER NOT NULL, "
                    + "max_y INTEGER NOT NULL, max_z INTEGER NOT NULL, completed_at INTEGER NOT NULL, "
                    + "blocks_format INTEGER NOT NULL DEFAULT " + BuildingVoxelCodec.FORMAT_V1 + ", "
                    + "blocks_payload BLOB, block_count INTEGER NOT NULL DEFAULT 0)");
            statement.executeUpdate("CREATE TABLE IF NOT EXISTS placed_building_pois(building_id TEXT NOT NULL, poi_key TEXT NOT NULL, "
                    + "poi_type TEXT NOT NULL, capacity INTEGER NOT NULL, world_x INTEGER NOT NULL, world_y INTEGER NOT NULL, "
                    + "world_z INTEGER NOT NULL, PRIMARY KEY(building_id, poi_key), "
                    + "FOREIGN KEY(building_id) REFERENCES placed_buildings(building_id) ON DELETE CASCADE)");
            statement.executeUpdate("CREATE INDEX IF NOT EXISTS idx_placed_buildings_city ON placed_buildings(city_id)");
            statement.executeUpdate("CREATE INDEX IF NOT EXISTS idx_placed_buildings_dimension ON placed_buildings(dimension_id)");
        }
    }

    /** createV1Baseline: 旧档幂等建出折叠前的三张表。 */
    private static void createV1Baseline(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("CREATE TABLE IF NOT EXISTS placed_buildings(building_id TEXT PRIMARY KEY, city_id TEXT, dimension_id TEXT NOT NULL, category TEXT NOT NULL, building_file_name TEXT NOT NULL, display_name TEXT NOT NULL, amount TEXT NOT NULL DEFAULT '', structure_file_name TEXT NOT NULL, facing TEXT NOT NULL, origin_x INTEGER NOT NULL, origin_y INTEGER NOT NULL, origin_z INTEGER NOT NULL, anchor_x INTEGER NOT NULL, anchor_y INTEGER NOT NULL, anchor_z INTEGER NOT NULL, min_x INTEGER NOT NULL, min_y INTEGER NOT NULL, min_z INTEGER NOT NULL, max_x INTEGER NOT NULL, max_y INTEGER NOT NULL, max_z INTEGER NOT NULL, completed_at INTEGER NOT NULL)");
            statement.executeUpdate("CREATE TABLE IF NOT EXISTS placed_building_blocks(building_id TEXT NOT NULL, relative_x INTEGER NOT NULL, relative_y INTEGER NOT NULL, relative_z INTEGER NOT NULL, block_id TEXT NOT NULL, block_state_nbt TEXT NOT NULL, original_x INTEGER NOT NULL, original_y INTEGER NOT NULL, original_z INTEGER NOT NULL, PRIMARY KEY(building_id, relative_x, relative_y, relative_z), FOREIGN KEY(building_id) REFERENCES placed_buildings(building_id) ON DELETE CASCADE)");
            statement.executeUpdate("CREATE TABLE IF NOT EXISTS placed_building_pois(building_id TEXT NOT NULL, poi_key TEXT NOT NULL, poi_type TEXT NOT NULL, capacity INTEGER NOT NULL, world_x INTEGER NOT NULL, world_y INTEGER NOT NULL, world_z INTEGER NOT NULL, PRIMARY KEY(building_id, poi_key), FOREIGN KEY(building_id) REFERENCES placed_buildings(building_id) ON DELETE CASCADE)");
            addColumnIfMissing(connection, "placed_buildings", "amount", "TEXT NOT NULL DEFAULT ''");
            statement.executeUpdate("CREATE INDEX IF NOT EXISTS idx_placed_buildings_city ON placed_buildings(city_id)");
            statement.executeUpdate("CREATE INDEX IF NOT EXISTS idx_placed_buildings_dimension ON placed_buildings(dimension_id)");
            statement.executeUpdate("CREATE INDEX IF NOT EXISTS idx_placed_blocks_building ON placed_building_blocks(building_id)");
        }
    }

    private static void backup(Path databasePath, int fromVersion) {
        try {
            Path backupDirectory = databasePath.getParent().resolve("backups");
            Files.createDirectories(backupDirectory);
            String fileName = databasePath.getFileName().toString();
            String stamp = LocalDateTime.now().format(BACKUP_STAMP);
            Path target = backupDirectory.resolve(fileName + ".v" + fromVersion + "-" + stamp + ".bak");
            Files.copy(databasePath, target, StandardCopyOption.REPLACE_EXISTING);
            SimuKraft.LOGGER.info("Simukraft: pre-migration building SQLite backup written to {}", target);
        } catch (IOException exception) {
            SimuKraft.LOGGER.warn("Simukraft: failed to write pre-migration building SQLite backup; continuing without it.", exception);
        }
    }

    private static void addColumnIfMissing(Connection connection, String tableName, String columnName, String columnDefinition) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("PRAGMA table_info(" + tableName + ")")) {
            while (resultSet.next()) {
                if (columnName.equalsIgnoreCase(resultSet.getString("name"))) {
                    return;
                }
            }
        }
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("ALTER TABLE " + tableName + " ADD COLUMN " + columnName + " " + columnDefinition);
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

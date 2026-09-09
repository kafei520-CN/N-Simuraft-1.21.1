package common.cn.kafei.simukraft.storage;

import common.cn.kafei.simukraft.building.BuildingBlockData;
import common.cn.kafei.simukraft.building.BuildingPoiDefinition;
import common.cn.kafei.simukraft.building.BuildingPoiInstance;
import common.cn.kafei.simukraft.building.PlacedBuildingRecord;
import common.cn.kafei.simukraft.city.poi.CityPoiType;
import net.minecraft.core.BlockPos;
import net.minecraft.core.registries.BuiltInRegistries;
import net.minecraft.world.level.block.Blocks;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Constructor;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BuildingStructureRepositoryTest {
    @TempDir
    Path tempDir;

    @Test
    void newDatabaseNeverCreatesBlockRowTable() throws Exception {
        try (BuildingStructureSqliteDatabase database = openDatabase(tempDir.resolve("fresh.sqlite"))) {
            try (Connection connection = database.borrowConnection();
                 Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(
                         "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'placed_building_blocks'")) {
                assertFalse(resultSet.next(), "全新档不得创建 placed_building_blocks");
            }
            assertEquals(2, userVersion(database));
        }
    }

    @Test
    void upsertRoundTripLoadsBlocksLazilyFromPayload() throws Exception {
        UUID buildingId = UUID.randomUUID();
        PlacedBuildingRecord record = building(buildingId, List.of(
                new BuildingBlockData(new BlockPos(1, 64, 2), Blocks.STONE.defaultBlockState(), new BlockPos(0, 0, 0)),
                new BuildingBlockData(new BlockPos(1, 65, 2), Blocks.OAK_PLANKS.defaultBlockState(), new BlockPos(0, 1, 0))
        ));
        Path path = tempDir.resolve("roundtrip.sqlite");
        try (BuildingStructureSqliteDatabase database = openDatabase(path)) {
            BuildingStructureRepository repository = new BuildingStructureRepository(database);
            assertEquals(BuildingStructureRepository.WriteOutcome.PERSISTED, repository.upsert(record));
            assertTrue(database.drainWrites());

            List<PlacedBuildingRecord> loaded = repository.loadByDimension("minecraft:overworld");
            assertNotNull(loaded);
            assertEquals(1, loaded.size());
            assertEquals(buildingId, loaded.getFirst().buildingId());
            assertEquals(2, loaded.getFirst().blocks().size());
            assertTrue(loaded.getFirst().blocks().getFirst().state().is(Blocks.STONE));
            assertTrue(loaded.getFirst().blocks().get(1).state().is(Blocks.OAK_PLANKS));
        }
    }

    @Test
    void legacyBlockRowsAreFoldedOnOpenAndRemainReadable() throws Exception {
        Path path = tempDir.resolve("legacy.sqlite");
        UUID kept = UUID.randomUUID();
        UUID second = UUID.randomUUID();
        createLegacyDatabase(path, List.of(legacy(kept, 1), legacy(second, 2)));

        try (BuildingStructureSqliteDatabase database = openDatabase(path)) {
            assertEquals(2, userVersion(database));
            try (Connection connection = database.borrowConnection();
                 Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(
                         "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'placed_building_blocks'")) {
                assertFalse(resultSet.next(), "折叠完成后必须 DROP placed_building_blocks");
            }
            BuildingStructureRepository repository = new BuildingStructureRepository(database);
            List<PlacedBuildingRecord> loaded = repository.loadByDimension("minecraft:overworld");
            assertNotNull(loaded);
            assertEquals(2, loaded.size());
            assertEquals(1, find(loaded, kept).blocks().size());
            assertEquals(1, find(loaded, second).blocks().size());
            assertTrue(find(loaded, kept).blocks().getFirst().state().is(Blocks.STONE));
        }
    }

    @Test
    void interruptedFoldResumesOnNextOpen() throws Exception {
        Path path = tempDir.resolve("resume.sqlite");
        UUID first = UUID.randomUUID();
        UUID second = UUID.randomUUID();
        createLegacyDatabase(path, List.of(legacy(first, 3), legacy(second, 4)));
        foldOnlyFirstBuilding(path, first);

        try (BuildingStructureSqliteDatabase database = openDatabase(path)) {
            BuildingStructureRepository repository = new BuildingStructureRepository(database);
            List<PlacedBuildingRecord> loaded = repository.loadByDimension("minecraft:overworld");
            assertNotNull(loaded);
            assertEquals(2, loaded.size());
            assertEquals(1, find(loaded, first).blocks().size());
            assertEquals(1, find(loaded, second).blocks().size());
            try (Connection connection = database.borrowConnection();
                 Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(
                         "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'placed_building_blocks'")) {
                assertFalse(resultSet.next());
            }
        }
    }

    private static PlacedBuildingRecord find(List<PlacedBuildingRecord> loaded, UUID buildingId) {
        return loaded.stream().filter(record -> record.buildingId().equals(buildingId)).findFirst().orElseThrow();
    }

    private static PlacedBuildingRecord building(UUID buildingId, List<BuildingBlockData> blocks) {
        return new PlacedBuildingRecord(
                buildingId, UUID.randomUUID(), "minecraft:overworld", "residential", "house.sk", "House", "",
                "house.nbt", "north", BlockPos.ZERO, BlockPos.ZERO, BlockPos.ZERO, new BlockPos(4, 4, 4), 1L,
                blocks, List.<BuildingPoiDefinition>of(),
                List.of(new BuildingPoiInstance("bed", CityPoiType.RESIDENTIAL, 1, new BlockPos(1, 1, 1))),
                List.of(), List.of());
    }

    private record LegacyBuilding(UUID id, int x) {
    }

    private static LegacyBuilding legacy(UUID id, int x) {
        return new LegacyBuilding(id, x);
    }

    private static void createLegacyDatabase(Path path, List<LegacyBuilding> buildings) throws Exception {
        Class.forName("org.sqlite.JDBC");
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + path.toAbsolutePath());
             Statement statement = connection.createStatement()) {
            statement.executeUpdate("CREATE TABLE placed_buildings(building_id TEXT PRIMARY KEY, city_id TEXT, dimension_id TEXT NOT NULL, category TEXT NOT NULL, building_file_name TEXT NOT NULL, display_name TEXT NOT NULL, amount TEXT NOT NULL DEFAULT '', structure_file_name TEXT NOT NULL, facing TEXT NOT NULL, origin_x INTEGER NOT NULL, origin_y INTEGER NOT NULL, origin_z INTEGER NOT NULL, anchor_x INTEGER NOT NULL, anchor_y INTEGER NOT NULL, anchor_z INTEGER NOT NULL, min_x INTEGER NOT NULL, min_y INTEGER NOT NULL, min_z INTEGER NOT NULL, max_x INTEGER NOT NULL, max_y INTEGER NOT NULL, max_z INTEGER NOT NULL, completed_at INTEGER NOT NULL)");
            statement.executeUpdate("CREATE TABLE placed_building_blocks(building_id TEXT NOT NULL, relative_x INTEGER NOT NULL, relative_y INTEGER NOT NULL, relative_z INTEGER NOT NULL, block_id TEXT NOT NULL, block_state_nbt TEXT NOT NULL, original_x INTEGER NOT NULL, original_y INTEGER NOT NULL, original_z INTEGER NOT NULL, PRIMARY KEY(building_id, relative_x, relative_y, relative_z))");
            statement.executeUpdate("CREATE TABLE placed_building_pois(building_id TEXT NOT NULL, poi_key TEXT NOT NULL, poi_type TEXT NOT NULL, capacity INTEGER NOT NULL, world_x INTEGER NOT NULL, world_y INTEGER NOT NULL, world_z INTEGER NOT NULL, PRIMARY KEY(building_id, poi_key))");
            for (LegacyBuilding building : buildings) {
                try (PreparedStatement insertBuilding = connection.prepareStatement(
                        "INSERT INTO placed_buildings(building_id, city_id, dimension_id, category, building_file_name, display_name, amount, structure_file_name, facing, origin_x, origin_y, origin_z, anchor_x, anchor_y, anchor_z, min_x, min_y, min_z, max_x, max_y, max_z, completed_at) VALUES(?, NULL, 'minecraft:overworld', 'residential', 'house.sk', 'House', '', 'house.nbt', 'north', 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 4, 4, 1)");
                     PreparedStatement insertBlock = connection.prepareStatement(
                             "INSERT INTO placed_building_blocks(building_id, relative_x, relative_y, relative_z, block_id, block_state_nbt, original_x, original_y, original_z) VALUES(?, ?, 64, 0, ?, ?, 0, 0, 0)")) {
                    insertBuilding.setString(1, building.id().toString());
                    insertBuilding.executeUpdate();
                    insertBlock.setString(1, building.id().toString());
                    insertBlock.setInt(2, building.x());
                    insertBlock.setString(3, BuiltInRegistries.BLOCK.getKey(Blocks.STONE).toString());
                    insertBlock.setString(4, BuildingVoxelCodec.encodeLegacyBlockState(Blocks.STONE.defaultBlockState()));
                    insertBlock.executeUpdate();
                }
            }
        }
    }

    private static void foldOnlyFirstBuilding(Path path, UUID firstId) throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + path.toAbsolutePath());
             Statement statement = connection.createStatement()) {
            statement.executeUpdate("ALTER TABLE placed_buildings ADD COLUMN blocks_format INTEGER NOT NULL DEFAULT 0");
            statement.executeUpdate("ALTER TABLE placed_buildings ADD COLUMN blocks_payload BLOB");
            statement.executeUpdate("ALTER TABLE placed_buildings ADD COLUMN block_count INTEGER NOT NULL DEFAULT 0");
            List<BuildingBlockData> blocks = List.of(
                    new BuildingBlockData(new BlockPos(3, 64, 0), Blocks.STONE.defaultBlockState(), BlockPos.ZERO));
            byte[] payload = BuildingVoxelCodec.encode(blocks);
            try (PreparedStatement update = connection.prepareStatement(
                    "UPDATE placed_buildings SET blocks_format = 1, blocks_payload = ?, block_count = 1 WHERE building_id = ?");
                 PreparedStatement delete = connection.prepareStatement(
                         "DELETE FROM placed_building_blocks WHERE building_id = ?")) {
                update.setBytes(1, payload);
                update.setString(2, firstId.toString());
                update.executeUpdate();
                delete.setString(1, firstId.toString());
                delete.executeUpdate();
            }
            statement.execute("PRAGMA user_version = 1");
        }
    }

    private static int userVersion(BuildingStructureSqliteDatabase database) throws Exception {
        try (Connection connection = database.borrowConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("PRAGMA user_version")) {
            resultSet.next();
            return resultSet.getInt(1);
        }
    }

    private static BuildingStructureSqliteDatabase openDatabase(Path databasePath) throws Exception {
        Constructor<BuildingStructureSqliteDatabase> constructor =
                BuildingStructureSqliteDatabase.class.getDeclaredConstructor(Path.class);
        constructor.setAccessible(true);
        return constructor.newInstance(databasePath);
    }
}

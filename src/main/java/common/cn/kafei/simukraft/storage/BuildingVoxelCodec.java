package common.cn.kafei.simukraft.storage;

import common.cn.kafei.simukraft.SimuKraft;
import common.cn.kafei.simukraft.building.BuildingBlockData;
import net.minecraft.core.BlockPos;
import net.minecraft.core.registries.BuiltInRegistries;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.NbtIo;
import net.minecraft.world.level.block.Block;
import net.minecraft.world.level.block.state.BlockState;
import net.minecraft.world.level.block.state.properties.Property;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * 已建成建筑体素快照的编解码。
 * <p>格式 v1：gzip( magic NSBK + version + 方块状态字典 + 坐标数组 )。空气不写入。
 */
public final class BuildingVoxelCodec {
    public static final int FORMAT_V1 = 1;
    private static final byte[] MAGIC = {'N', 'S', 'B', 'K'};

    private BuildingVoxelCodec() {
    }

    /** solidCount: 会写入快照的非空气方块数。 */
    public static int solidCount(List<BuildingBlockData> blocks) {
        return solidBlocks(blocks).size();
    }

    /** encode: 把完工快照压成 payload；空列表写成合法的空快照而不是 null。 */
    public static byte[] encode(List<BuildingBlockData> blocks) {
        List<BuildingBlockData> solid = solidBlocks(blocks);
        Map<BlockState, Integer> palette = new LinkedHashMap<>();
        for (BuildingBlockData block : solid) {
            palette.putIfAbsent(block.state(), palette.size());
        }
        if (palette.size() > 65535) {
            throw new IllegalArgumentException("Building voxel palette exceeds 65535 unique block states");
        }
        ByteArrayOutputStream gzipBytes = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(gzipBytes);
             DataOutputStream data = new DataOutputStream(gzip)) {
            data.write(MAGIC);
            data.writeByte(FORMAT_V1);
            data.writeShort(palette.size());
            for (BlockState state : palette.keySet()) {
                writeState(data, state);
            }
            data.writeInt(solid.size());
            for (BuildingBlockData block : solid) {
                data.writeInt(block.relativePos().getX());
                data.writeInt(block.relativePos().getY());
                data.writeInt(block.relativePos().getZ());
                data.writeInt(block.originalStructurePos().getX());
                data.writeInt(block.originalStructurePos().getY());
                data.writeInt(block.originalStructurePos().getZ());
                data.writeShort(palette.get(block.state()));
            }
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to encode building voxel snapshot", exception);
        }
        return gzipBytes.toByteArray();
    }

    /** decode: 空/损坏的 payload 返回空列表，不抛到加载路径。 */
    public static List<BuildingBlockData> decode(byte[] payload) {
        if (payload == null || payload.length == 0) {
            return List.of();
        }
        try (DataInputStream data = new DataInputStream(new GZIPInputStream(new ByteArrayInputStream(payload)))) {
            byte[] magic = data.readNBytes(4);
            if (magic.length != 4 || magic[0] != MAGIC[0] || magic[1] != MAGIC[1]
                    || magic[2] != MAGIC[2] || magic[3] != MAGIC[3]) {
                SimuKraft.LOGGER.error("Simukraft: building voxel snapshot has invalid magic");
                return List.of();
            }
            int version = data.readUnsignedByte();
            if (version != FORMAT_V1) {
                SimuKraft.LOGGER.error("Simukraft: unsupported building voxel snapshot version {}", version);
                return List.of();
            }
            int paletteSize = data.readUnsignedShort();
            BlockState[] palette = new BlockState[paletteSize];
            for (int index = 0; index < paletteSize; index++) {
                palette[index] = readState(data);
            }
            int blockCount = data.readInt();
            if (blockCount < 0) {
                SimuKraft.LOGGER.error("Simukraft: building voxel snapshot has negative block count");
                return List.of();
            }
            List<BuildingBlockData> blocks = new ArrayList<>(blockCount);
            for (int index = 0; index < blockCount; index++) {
                int relX = data.readInt();
                int relY = data.readInt();
                int relZ = data.readInt();
                int origX = data.readInt();
                int origY = data.readInt();
                int origZ = data.readInt();
                int stateIndex = data.readUnsignedShort();
                if (stateIndex >= palette.length) {
                    continue;
                }
                BlockState state = palette[stateIndex];
                if (state == null || state.isAir()) {
                    continue;
                }
                blocks.add(new BuildingBlockData(new BlockPos(relX, relY, relZ), state, new BlockPos(origX, origY, origZ)));
            }
            return List.copyOf(blocks);
        } catch (Exception exception) {
            SimuKraft.LOGGER.error("Simukraft: failed to decode building voxel snapshot", exception);
            return List.of();
        }
    }

    /** decodeLegacyBlockState: 旧表 Base64(NBT) 行，迁移折叠时用。 */
    public static BlockState decodeLegacyBlockState(String encoded, String blockId) {
        try {
            byte[] bytes = Base64.getDecoder().decode(encoded == null ? "" : encoded);
            CompoundTag tag = NbtIo.read(new DataInputStream(new ByteArrayInputStream(bytes)));
            if (tag == null) {
                return fallbackState(blockId);
            }
            String name = tag.getString("Name");
            Block block = BuiltInRegistries.BLOCK.getOptional(net.minecraft.resources.ResourceLocation.parse(name)).orElse(null);
            if (block == null) {
                return fallbackState(blockId);
            }
            BlockState state = block.defaultBlockState();
            if (tag.contains("Properties", net.minecraft.nbt.Tag.TAG_COMPOUND)) {
                CompoundTag properties = tag.getCompound("Properties");
                for (String key : properties.getAllKeys()) {
                    Property<?> property = state.getBlock().getStateDefinition().getProperty(key);
                    if (property != null) {
                        state = applyProperty(state, property, properties.getString(key));
                    }
                }
            }
            return state;
        } catch (Exception exception) {
            SimuKraft.LOGGER.warn("Failed to decode stored block state for {}; falling back to its default state", blockId, exception);
            return fallbackState(blockId);
        }
    }

    /** encodeLegacyBlockState: 仅测试用来构造旧表行。 */
    public static String encodeLegacyBlockState(BlockState state) {
        CompoundTag tag = new CompoundTag();
        tag.putString("Name", BuiltInRegistries.BLOCK.getKey(state.getBlock()).toString());
        CompoundTag properties = new CompoundTag();
        for (Property<?> property : state.getProperties()) {
            properties.putString(property.getName(), state.getValue(property).toString());
        }
        tag.put("Properties", properties);
        try (ByteArrayOutputStream output = new ByteArrayOutputStream(); DataOutputStream data = new DataOutputStream(output)) {
            NbtIo.write(tag, data);
            return Base64.getEncoder().encodeToString(output.toByteArray());
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to encode legacy block state", exception);
        }
    }

    private static List<BuildingBlockData> solidBlocks(List<BuildingBlockData> blocks) {
        if (blocks == null || blocks.isEmpty()) {
            return List.of();
        }
        List<BuildingBlockData> solid = new ArrayList<>(blocks.size());
        for (BuildingBlockData block : blocks) {
            if (block == null || block.state() == null || block.state().isAir()
                    || block.relativePos() == null || block.originalStructurePos() == null) {
                continue;
            }
            solid.add(block);
        }
        return solid;
    }

    private static void writeState(DataOutputStream data, BlockState state) throws IOException {
        data.writeUTF(BuiltInRegistries.BLOCK.getKey(state.getBlock()).toString());
        var properties = state.getProperties();
        data.writeByte(properties.size());
        for (Property<?> property : properties) {
            data.writeUTF(property.getName());
            data.writeUTF(state.getValue(property).toString());
        }
    }

    private static BlockState readState(DataInputStream data) throws IOException {
        String blockId = data.readUTF();
        int propertyCount = data.readUnsignedByte();
        String[] keys = new String[propertyCount];
        String[] values = new String[propertyCount];
        for (int index = 0; index < propertyCount; index++) {
            keys[index] = data.readUTF();
            values[index] = data.readUTF();
        }
        Block block = BuiltInRegistries.BLOCK.getOptional(net.minecraft.resources.ResourceLocation.parse(blockId)).orElse(null);
        if (block == null) {
            return null;
        }
        BlockState state = block.defaultBlockState();
        for (int index = 0; index < propertyCount; index++) {
            Property<?> property = state.getBlock().getStateDefinition().getProperty(keys[index]);
            if (property != null) {
                state = applyProperty(state, property, values[index]);
            }
        }
        return state;
    }

    private static BlockState fallbackState(String blockId) {
        if (blockId == null || blockId.isBlank()) {
            return null;
        }
        Block block = BuiltInRegistries.BLOCK.getOptional(net.minecraft.resources.ResourceLocation.parse(blockId)).orElse(null);
        return block != null ? block.defaultBlockState() : null;
    }

    private static <T extends Comparable<T>> BlockState applyProperty(BlockState state, Property<T> property, String value) {
        return property.getValue(value).map(parsed -> state.setValue(property, parsed)).orElse(state);
    }
}

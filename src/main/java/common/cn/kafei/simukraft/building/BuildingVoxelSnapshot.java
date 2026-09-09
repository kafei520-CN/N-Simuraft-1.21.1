package common.cn.kafei.simukraft.building;

import common.cn.kafei.simukraft.SimuKraft;

import java.util.List;
import java.util.function.Supplier;

/**
 * 已建成建筑的体素快照。内存里可以是现成列表，也可以在第一次 {@link #blocks()} 时再解码。
 */
public final class BuildingVoxelSnapshot {
    private static final BuildingVoxelSnapshot EMPTY = new BuildingVoxelSnapshot(List.of(), null);

    private final List<BuildingBlockData> eager;
    private final Supplier<List<BuildingBlockData>> loader;
    private volatile List<BuildingBlockData> decoded;

    private BuildingVoxelSnapshot(List<BuildingBlockData> eager, Supplier<List<BuildingBlockData>> loader) {
        this.eager = eager;
        this.loader = loader;
    }

    public static BuildingVoxelSnapshot empty() {
        return EMPTY;
    }

    /** of: 已经在内存中的方块列表（完工登记、搬家后的新快照）。 */
    public static BuildingVoxelSnapshot of(List<BuildingBlockData> blocks) {
        if (blocks == null || blocks.isEmpty()) {
            return EMPTY;
        }
        return new BuildingVoxelSnapshot(List.copyOf(blocks), null);
    }

    /** lazy: 第一次访问时才从库里读 payload。 */
    public static BuildingVoxelSnapshot lazy(Supplier<List<BuildingBlockData>> loader) {
        if (loader == null) {
            return EMPTY;
        }
        return new BuildingVoxelSnapshot(null, loader);
    }

    public List<BuildingBlockData> blocks() {
        if (eager != null) {
            return eager;
        }
        List<BuildingBlockData> cached = decoded;
        if (cached != null) {
            return cached;
        }
        synchronized (this) {
            if (decoded == null) {
                try {
                    List<BuildingBlockData> loaded = loader.get();
                    decoded = loaded == null || loaded.isEmpty() ? List.of() : List.copyOf(loaded);
                } catch (RuntimeException exception) {
                    SimuKraft.LOGGER.error("Simukraft: failed to lazy-load building voxel snapshot", exception);
                    decoded = List.of();
                }
            }
            return decoded;
        }
    }
}

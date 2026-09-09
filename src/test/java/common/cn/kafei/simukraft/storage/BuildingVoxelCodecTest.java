package common.cn.kafei.simukraft.storage;

import common.cn.kafei.simukraft.building.BuildingBlockData;
import net.minecraft.core.BlockPos;
import net.minecraft.core.Direction;
import net.minecraft.world.level.block.Blocks;
import net.minecraft.world.level.block.StairBlock;
import net.minecraft.world.level.block.state.BlockState;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BuildingVoxelCodecTest {

    @Test
    void roundTripKeepsBlockStatesAndSkipsAir() {
        BlockState stairs = Blocks.OAK_STAIRS.defaultBlockState().setValue(StairBlock.FACING, Direction.EAST);
        List<BuildingBlockData> source = List.of(
                new BuildingBlockData(new BlockPos(1, 2, 3), Blocks.STONE.defaultBlockState(), new BlockPos(4, 5, 6)),
                new BuildingBlockData(new BlockPos(7, 8, 9), Blocks.AIR.defaultBlockState(), new BlockPos(10, 11, 12)),
                new BuildingBlockData(new BlockPos(0, 1, 0), stairs, new BlockPos(0, 0, 0))
        );

        List<BuildingBlockData> decoded = BuildingVoxelCodec.decode(BuildingVoxelCodec.encode(source));

        assertEquals(2, decoded.size());
        assertEquals(2, BuildingVoxelCodec.solidCount(source));
        assertEquals(new BlockPos(1, 2, 3), decoded.get(0).relativePos());
        assertTrue(decoded.get(0).state().is(Blocks.STONE));
        assertEquals(new BlockPos(4, 5, 6), decoded.get(0).originalStructurePos());
        assertTrue(decoded.get(1).state().is(Blocks.OAK_STAIRS));
        assertEquals(Direction.EAST, decoded.get(1).state().getValue(StairBlock.FACING));
    }

    @Test
    void emptyAndNullPayloadDecodeToEmptyList() {
        assertTrue(BuildingVoxelCodec.decode(null).isEmpty());
        assertTrue(BuildingVoxelCodec.decode(new byte[0]).isEmpty());
        assertTrue(BuildingVoxelCodec.decode(BuildingVoxelCodec.encode(List.of())).isEmpty());
    }

    @Test
    void lazySnapshotDoesNotLoadUntilBlocksAreRead() {
        AtomicInteger loads = new AtomicInteger();
        var snapshot = common.cn.kafei.simukraft.building.BuildingVoxelSnapshot.lazy(() -> {
            loads.incrementAndGet();
            return List.of(new BuildingBlockData(BlockPos.ZERO, Blocks.STONE.defaultBlockState(), BlockPos.ZERO));
        });
        assertEquals(0, loads.get());
        assertEquals(1, snapshot.blocks().size());
        assertEquals(1, snapshot.blocks().size());
        assertEquals(1, loads.get(), "体素只应解码一次");
    }
}

package org.mitallast.queue.common.collection;

import org.junit.Assert;
import org.junit.Test;
import org.mitallast.queue.common.BaseTest;

import java.util.Random;

public class ImmutableIntMapTest extends BaseTest {

    @Test
    public void test() throws Exception {
        for (int t = 0; t < 100; t++) {
            long seed = this.random.nextLong();
            Random random = new Random(seed);
            ImmutableIntMapBuilder<Integer> builder = ImmutableIntMap.builder();
            for (int i = random.nextInt(100000); i-- > 0; ) {
                builder.put(random.nextInt(), random.nextInt());
            }

            ImmutableIntMap<Integer> map = builder.build();
            builder = ImmutableIntMap.builder();
            ImmutableIntMap<Integer> copy = builder.putAll(map).build();

            map.forEach((k, v) -> {
                Assert.assertTrue(copy.containsKey(k));
                Assert.assertEquals(v, copy.get(k));
            });
        }
    }
}

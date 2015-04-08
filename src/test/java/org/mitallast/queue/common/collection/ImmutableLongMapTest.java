package org.mitallast.queue.common.collection;

import org.junit.Assert;
import org.junit.Test;
import org.mitallast.queue.common.BaseTest;

import java.util.Random;

public class ImmutableLongMapTest extends BaseTest {

    @Test
    public void test() throws Exception {
        for (int t = 0; t < 100; t++) {
            long seed = this.random.nextLong();
            Random random = new Random(seed);
            ImmutableLongMapBuilder<Long> builder = ImmutableLongMap.builder();
            for (int i = random.nextInt(100000); i-- > 0; ) {
                builder.put(random.nextLong(), random.nextLong());
            }

            ImmutableLongMap<Long> map = builder.build();
            builder = ImmutableLongMap.builder();
            ImmutableLongMap<Long> copy = builder.putAll(map).build();

            map.forEach((k, v) -> {
                Assert.assertTrue(copy.containsKey(k));
                Assert.assertEquals(v, copy.get(k));
            });
        }
    }
}

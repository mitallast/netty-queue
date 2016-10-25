package org.mitallast.queue.common.collection;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;
import org.mitallast.queue.common.BaseTest;
import org.mitallast.queue.common.Immutable;

import java.util.Random;

public class ImmutableClassIntMapTest extends BaseTest {

    @Test
    public void testEmpty() throws Exception {
        ImmutableClassIntMap map = ImmutableClassIntMap.EMPTY;
        Assert.assertEquals(0, map.size());
        Assert.assertTrue(map.isEmpty());
        Assert.assertFalse(map.containsKey(this.getClass()));
        Assert.assertEquals(HashFunctions.emptyValue, map.get(this.getClass()));
    }

    @Test
    public void test() throws Exception {
        for (int t = 0; t < 100; t++) {
            long seed = this.random.nextLong();
            Random random = new Random(seed);
            ImmutableClassIntMapBuilder builder = ImmutableClassIntMap.builder();

            for (int i = random.nextInt(classes.size()); i-- > 0; ) {
                Class type = classes.get(random.nextInt(classes.size()));
                builder.put(type, random.nextInt());
            }

            ImmutableClassIntMap map = builder.build();
            builder = ImmutableClassIntMap.builder();
            ImmutableClassIntMap copy = builder.putAll(map).build();

            map.forEach((k, v) -> {
                Assert.assertTrue(copy.containsKey(k));
                Assert.assertEquals(v, copy.get(k));
            });
        }
    }

    private static ImmutableList<Class<?>> classes = ImmutableList.of(
        Test1.class,
        Test2.class,
        Test3.class,
        Test4.class,
        Test5.class,
        Test6.class,
        Test7.class,
        Test8.class,
        Test9.class,
        Test10.class,
        Test11.class,
        Test12.class,
        Test13.class,
        Test14.class,
        Test15.class,
        Test16.class,
        Test17.class,
        Test18.class,
        Test19.class,
        Test20.class
    );

    static class Test1{}
    static class Test2{}
    static class Test3{}
    static class Test4{}
    static class Test5{}
    static class Test6{}
    static class Test7{}
    static class Test8{}
    static class Test9{}
    static class Test10{}
    static class Test11{}
    static class Test12{}
    static class Test13{}
    static class Test14{}
    static class Test15{}
    static class Test16{}
    static class Test17{}
    static class Test18{}
    static class Test19{}
    static class Test20{}
}

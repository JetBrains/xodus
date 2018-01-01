/**
 * Copyright 2010 - 2018 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.log;

import jetbrains.exodus.*;
import jetbrains.exodus.util.Random;
import org.junit.Assert;
import org.junit.Test;

public class LoggableTests {

    @Test
    public void testCompressedLongByteIterator() {
        Assert.assertTrue(matchesArray(CompressedUnsignedLongByteIterable.getIterable(0), new byte[]{-128}));
        Assert.assertTrue(matchesArray(CompressedUnsignedLongByteIterable.getIterable(1), new byte[]{-127}));
        Assert.assertTrue(matchesArray(CompressedUnsignedLongByteIterable.getIterable(128), new byte[]{0, -127}));
        Assert.assertTrue(matchesArray(CompressedUnsignedLongByteIterable.getIterable(16384), new byte[]{0, 0, -127}));
        Assert.assertTrue(matchesArray(CompressedUnsignedLongByteIterable.getIterable(16383), new byte[]{127, -1}));
        Assert.assertTrue(matchesArray(CompressedUnsignedLongByteIterable.getIterable(25000), new byte[]{40, 67, -127}));
        Assert.assertTrue(matchesArray(CompressedUnsignedLongByteIterable.getIterable(Long.MAX_VALUE), new byte[]{127, 127, 127, 127, 127, 127, 127, 127, -1}));
    }

    @Test
    public void testCompressedLongByteIterator2() {
        final Random rnd = new Random();
        for (int i = 0; i < 10000; ++i) {
            final long l = Math.abs(rnd.nextLong());
            Assert.assertEquals(l, CompressedUnsignedLongByteIterable.getLong(CompressedUnsignedLongByteIterable.getIterable(l)));
        }
    }

    @Test
    public void testFactoryNullLoggable() {
        final Loggable nullLoggable = NullLoggable.create();
        Assert.assertNotNull(nullLoggable);
        Assert.assertEquals(nullLoggable.getType(), NullLoggable.create().getType());
    }

    @Test
    public void testCompoundByteIterable1() {
        Assert.assertTrue(matchesArray(
                new CompoundByteIterable(new ByteIterable[]{
                        new ArrayByteIterable(new byte[]{})
                }), new byte[]{}
        ));

        Assert.assertTrue(matchesArray(
                new CompoundByteIterable(new ByteIterable[]{
                        new ArrayByteIterable(new byte[]{}),
                        new ArrayByteIterable(new byte[]{})
                }), new byte[]{}
        ));
    }

    @Test
    public void testCompoundByteIterable2() {
        Assert.assertTrue(matchesArray(
                new CompoundByteIterable(new ByteIterable[]{
                        new ArrayByteIterable(new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
                }), new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
        ));

        Assert.assertTrue(matchesArray(
                new CompoundByteIterable(new ByteIterable[]{
                        new ArrayByteIterable(new byte[]{}),
                        new ArrayByteIterable(new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
                }), new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
        ));

        Assert.assertTrue(matchesArray(
                new CompoundByteIterable(new ByteIterable[]{
                        new ArrayByteIterable(new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}),
                        new ArrayByteIterable(new byte[]{})
                }), new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
        ));

        Assert.assertTrue(matchesArray(
                new CompoundByteIterable(new ByteIterable[]{
                        new ArrayByteIterable(new byte[]{}),
                        new ArrayByteIterable(new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}),
                        new ArrayByteIterable(new byte[]{})
                }), new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
        ));
    }

    @Test
    public void testCompoundByteIterable3() {
        final CompoundByteIterable ci = new CompoundByteIterable(new ByteIterable[]{
                new ArrayByteIterable(new byte[]{0, 1, 2, 3, 4}),
                new ArrayByteIterable(new byte[]{5, 6, 7, 8, 9})
        });
        Assert.assertEquals(10, ci.getLength());

        Assert.assertTrue(matchesArray(
                ci, new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
    }

    @Test
    public void testCompoundByteIteratorSkip2() {
        CompoundByteIteratorBase iterator = new CompoundByteIteratorBase() {
            byte[][] a = {{1}, {2}, {3}, {4}};
            int current = 0;

            @Override
            protected ByteIterator nextIterator() {
                return new ArrayByteIterable(a[current++]).iterator();
            }
        };
        iterator.skip(3);
        Assert.assertEquals(iterator.next(), 4);
    }

    @Test
    public void testCompoundByteIterator() {
        Assert.assertTrue(matchesArray(new ByteIterableBase() {

            @Override
            protected ByteIterator getIterator() {

                return new CompoundByteIteratorBase() {
                    byte[] array1 = {0, 1, 2, 3};
                    byte[] array2 = {4, 5, 6, 7, 8, 9};

                    @Override
                    protected ByteIterator nextIterator() {
                        final byte[] a1 = array1;
                        if (a1 != null) {
                            array1 = null;
                            return new ArrayByteIterable(a1).iterator();
                        }
                        final byte[] a2 = array2;
                        if (a2 != null) {
                            array2 = null;
                            return new ArrayByteIterable(a2).iterator();
                        }
                        return null;
                    }
                };
            }
        }, new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
    }

    @Test
    public void testCompoundByteIterator2() {
        Assert.assertTrue(matchesArray(new ByteIterableBase() {

            @Override
            protected ByteIterator getIterator() {

                return new CompoundByteIteratorBase() {
                    byte[] array1 = {};
                    byte[] array2 = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

                    @Override
                    protected ByteIterator nextIterator() {
                        final byte[] a1 = array1;
                        if (a1 != null) {
                            array1 = null;
                            return new ArrayByteIterable(a1).iterator();
                        }
                        final byte[] a2 = array2;
                        if (a2 != null) {
                            array2 = null;
                            return new ArrayByteIterable(a2).iterator();
                        }
                        return null;
                    }
                };
            }
        }, new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
    }

    @Test
    public void testCompoundByteIterator3() {
        Assert.assertTrue(matchesArray(new ByteIterableBase() {

            @Override
            protected ByteIterator getIterator() {

                return new CompoundByteIteratorBase() {
                    byte[] array1 = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
                    byte[] array2 = {};

                    @Override
                    protected ByteIterator nextIterator() {
                        final byte[] a1 = array1;
                        if (a1 != null) {
                            array1 = null;
                            return new ArrayByteIterable(a1).iterator();
                        }
                        final byte[] a2 = array2;
                        if (a2 != null) {
                            array2 = null;
                            return new ArrayByteIterable(a2).iterator();
                        }
                        return null;
                    }
                };
            }
        }, new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
    }

    @Test
    public void testCompoundByteIteratorN() {
        Assert.assertTrue(matchesArray(new CompoundByteArrayIterable(new ByteIterator[]{
                new ArrayByteIterable(new byte[]{0, 1, 2}).iterator(),
                new ArrayByteIterable(new byte[]{3, 4, 5}).iterator(),
                new ArrayByteIterable(new byte[]{6, 7, 8, 9}).iterator(),
                new ArrayByteIterable(new byte[0]).iterator()
        }), new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
    }

    @Test
    public void testCompoundByteIteratorSkip() {
        ByteIterator it = new CompoundByteArrayIterable(new ByteIterator[]{
                new ArrayByteIterable(new byte[]{0, 1, 2}).iterator(),
                new ArrayByteIterable(new byte[]{3, 4, 5}).iterator(),
                new ArrayByteIterable(new byte[]{6, 7, 8, 9}).iterator(),
                new ArrayByteIterable(new byte[0]).iterator()
        }).iterator();
        // skip first two elements
        it.next();
        it.next();
        Assert.assertEquals(2L, it.skip(2));
        Assert.assertEquals((byte) 4, it.next());
        Assert.assertEquals(5L, it.skip(6));
    }

    private boolean matchesArray(ByteIterable iterable, byte[] array) {
        final ByteIterator iterator = iterable.iterator();
        int i = 0;
        while (iterator.hasNext()) {
            if (i == array.length) {
                return false;
            }
            final byte b = iterator.next();
            if (b != array[i]) {
                System.out.println(i + ": " + b + " != " + array[i]);
                return false;
            }
            ++i;
        }
        return i == array.length;
    }

    private static class CompoundByteArrayIterable extends ByteIterableBase {

        private final ByteIterator[] iterators;
        private final int count;
        private final int offset;

        private CompoundByteArrayIterable(ByteIterator[] iterators) {
            this.iterators = iterators;
            count = iterators.length;
            offset = 0;
        }

        @Override
        protected ByteIterator getIterator() {
            return new CompoundByteIteratorBase(iterators[0]) {
                int off = offset;

                @Override
                public ByteIterator nextIterator() {
                    return off < count ? iterators[off++] : null;
                }
            };
        }

    }
}

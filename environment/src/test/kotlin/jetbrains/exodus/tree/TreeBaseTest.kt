/*
 * Copyright 2010 - 2023 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.tree

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.ExodusException
import jetbrains.exodus.TestUtil
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.env.EnvironmentConfig
import jetbrains.exodus.env.Environments.newLogInstance
import jetbrains.exodus.log.*
import jetbrains.exodus.tree.btree.LeafNodeKV
import jetbrains.exodus.util.IOUtil.deleteFile
import jetbrains.exodus.util.IOUtil.deleteRecursively
import jetbrains.exodus.util.Random
import org.junit.After
import org.junit.Assert
import org.junit.Before
import java.io.File
import java.text.DecimalFormat
import java.text.NumberFormat
import java.util.*

abstract class TreeBaseTest<T : ITree, MT : ITreeMutable> {
    open var tree: T? = null
        protected set
    open var treeMutable: MT? = null
        protected set

    protected abstract fun createMutableTree(hasDuplicates: Boolean, structureId: Int): MT?
    protected abstract fun openTree(address: Long, hasDuplicates: Boolean): ITree?
    @Before
    fun start() {
        val userHome = System.getProperty("user.home")
        if (userHome == null || userHome.isEmpty()) {
            throw ExodusException("user.home is undefined.")
        }
        tempFolder = TestUtil.createTempDir()
        createLog()
    }

    private fun createLog() {
        val config = createLogConfig()
        config!!.setLocation(tempFolder!!.path)
        log = newLogInstance(config)
    }

    protected open fun createLogConfig(): LogConfig? {
        return LogConfig().setNonBlockingCache(true).setReaderWriterProvider(
            EnvironmentConfig.DEFAULT.logDataReaderWriterProvider
        )
    }

    @After
    fun end() {
        log!!.close()
        deleteRecursively(tempFolder!!)
        deleteFile(tempFolder!!)
    }

    protected fun reopen() {
        log!!.close()
        createLog()
    }

    @Suppress("SameParameterValue")
    protected fun createLNs(s: Int): MutableList<INode> {
        return createLNs("v", s)
    }

    protected fun createLNs(valuePrefix: String, s: Int): MutableList<INode> {
        val l: MutableList<INode> = ArrayList()
        for (i in 0 until s) {
            l.add(kv(i, valuePrefix + i))
        }
        return l
    }

    protected fun createLNs(valuePrefix: String, s: Int, u: Int): List<INode> {
        val l: MutableList<INode> = ArrayList()
        for (i in 0 until s) {
            for (j in 0 until u) {
                l.add(kv(i, "$valuePrefix$i#$j"))
            }
        }
        return l
    }

    protected fun checkTree(bt: ITree, s: Int): TreeAwareRunnable {
        return checkTree(bt, "v", s)
    }

    protected fun checkTree(bt: ITree, valuePrefix: String, s: Int): TreeAwareRunnable {
        return object : TreeAwareRunnable(bt) {
            override fun run() {
                val l = createLNs(valuePrefix, s)
                assertMatchesIterator(t!!, l)
            }
        }
    }

    protected fun checkTree(bt: ITree, s: Int, u: Int): TreeAwareRunnable {
        return checkTree(bt, "v", s, u)
    }

    @Suppress("SameParameterValue")
    private fun checkTree(bt: ITree, valuePrefix: String, s: Int, u: Int): TreeAwareRunnable {
        return object : TreeAwareRunnable(bt) {
            override fun run() {
                val l = createLNs(valuePrefix, s, u)
                assertMatchesIterator(t!!, l)
            }
        }
    }

    fun saveTree(): Long {
        log!!.beginWrite()
        val result = treeMutable!!.save()
        log!!.flush()
        log!!.endWrite()
        return result
    }

    fun dump(@Suppress("UNUSED_PARAMETER") t: ITree?) {
        /*t.dump(System.out, new INode.ToString() {
            @Override
            public String toString(INode ln) {
                final StringBuilder sb = new StringBuilder(16);
                sb.append(new String(ln.getKey().getBytesUnsafe(), 0, ln.getKey().getLength()));
                if (ln.hasValue()) {
                    sb.append(':');
                    sb.append(new String(ln.getValue().getBytesUnsafe(), 0, ln.getValue().getLength()));
                }
                return sb.toString();
            }
        });*/
    }

    abstract class TreeAwareRunnable {
        protected var t: ITree? = null

        protected constructor(t: ITree?) {
            this.t = t
        }

        protected constructor()

        fun setTree(tree: ITree?) {
            t = tree
        }

        abstract fun run()
    }

    companion object {
        private var FORMATTER: DecimalFormat? = null
        var RANDOM: Random? = null

        init {
            FORMATTER = NumberFormat.getIntegerInstance() as DecimalFormat
            FORMATTER!!.applyPattern("00000")
            RANDOM = Random(77634963005211L)
        }

        var log: Log? = null
        protected var tempFolder: File? = null
        fun key(key: String): ArrayByteIterable {
            return ArrayByteIterable(key.toByteArray())
        }

        fun value(value: String): ByteIterable {
            return key(value)
        }

        fun key(key: Int): ArrayByteIterable {
            return key(FORMATTER!!.format(key.toLong()))
        }

        fun key(key: Long): ArrayByteIterable {
            return LongBinding.longToEntry(key)
        }

        fun value(value: Long): ByteIterable {
            return key(value)
        }

        fun valueEquals(expectedValue: String?, ln: ByteIterable?) {
            Assert.assertEquals(expectedValue, String(ln!!.bytesUnsafe, 0, ln.length))
        }

        fun checkEmptyTree(bt: ITree) {
            Assert.assertEquals(log, bt.log)
            Assert.assertTrue(bt.isEmpty)
            Assert.assertEquals(0, bt.size)
            Assert.assertNull(bt[ByteIterable.EMPTY])
            Assert.assertNull(bt[key("some key")])
            Assert.assertFalse(bt.openCursor().next)
            Assert.assertFalse(bt.hasKey(key("some key")))
            Assert.assertFalse(bt.hasPair(key("some key"), value("some value")))
        }

        fun kv(key: String): INode {
            return StringKVNode(key, "")
        }

        fun kv(key: String, value: String?): INode {
            return StringKVNode(key, value)
        }

        fun kv(key: Int, value: String?): INode {
            return kv(FORMATTER!!.format(key.toLong()), value)
        }

        fun v(value: Int): ByteIterable {
            return value("val " + FORMATTER!!.format(value.toLong()))
        }

        fun assertMatchesIterator(actual: ITree, expected: List<INode>) {
            assertMatchesIterator(actual, *expected.toTypedArray<INode>())
        }

        fun assertMatchesIteratorAndExists(actual: ITree, vararg expected: INode) {
            assertMatchesIterator(actual, true, *expected)
        }

        fun assertMatchesIterator(actual: ITree, vararg expected: INode) {
            assertMatchesIterator(actual, false, *expected)
        }

        fun assertMatchesIterator(actual: ITree, checkExists: Boolean, vararg expected: INode) {
            val it1 = actual.openCursor()
            val act: MutableList<INode> = ArrayList(actual.size.toInt())
            while (it1.next) {
                act.add(LeafNodeKV(it1.key, it1.value))
            }
            Assert.assertArrayEquals(expected, act.toTypedArray<INode>())
            if (checkExists) {
                for (leafNode in expected) {
                    Assert.assertTrue(actual.hasPair(leafNode.key, leafNode.value!!))
                }
            }
        }

        fun assertIterablesMatch(expected: ByteIterable?, actual: ByteIterable?) {
            if (expected == null) {
                Assert.assertNull(actual)
            } else {
                Assert.assertNotNull(actual)
                val expItr = expected.iterator()
                val actItr = actual!!.iterator()
                while (expItr.hasNext()) {
                    Assert.assertTrue(actItr.hasNext())
                    Assert.assertEquals(expItr.next().toLong(), actItr.next().toLong())
                }
                Assert.assertFalse(actItr.hasNext())
            }
        }

        fun checkAddressSet(tree: ITree, count: Int) {
            val addressIterator = tree.addressIterator()
            val list: MutableList<Long> = ArrayList(count)
            while (addressIterator.hasNext()) {
                val l = addressIterator.next()
                list.add(l)
            }
            Assert.assertEquals(count.toLong(), list.size.toLong())
            list.sort()
            var prev = -239L
            for (l in list) {
                Assert.assertNotEquals(prev, l)
                prev = l
            }
        }

        fun doDeleteViaCursor(testCase: TreeBaseTest<ITree, ITreeMutable>, key: ByteIterable) {
            val cursor = testCase.treeMutable!!.openCursor()
            Assert.assertNotNull(cursor.getSearchKey(key))
            Assert.assertTrue(cursor.deleteCurrent())
        }
    }
}

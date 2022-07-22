/**
 * Copyright 2010 - 2022 JetBrains s.r.o.
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
package jetbrains.exodus.tree.patricia

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.ExodusException
import jetbrains.exodus.core.dataStructures.hash.HashSet
import jetbrains.exodus.kotlin.notNull
import jetbrains.exodus.log.*
import jetbrains.exodus.tree.*
import jetbrains.exodus.tree.ExpiredLoggableCollection.Companion.EMPTY
import java.util.*

internal class PatriciaTreeMutable(
    log: Log,
    structureId: Int,
    treeSize: Long,
    immutableRoot: ImmutableNode
) : PatriciaTreeBase(log, structureId), ITreeMutableExt {

    private var root = MutableRoot(immutableRoot)
    private var expiredLoggables: ExpiredLoggableCollection? = null
    private var openCursors: MutableSet<ITreeCursorMutable>? = null

    init {
        size = treeSize
        addExpiredLoggable(immutableRoot.loggable)
    }

    val useV1Format: Boolean get() = log.config.useV1Format()

    override fun getRootAddress() = Loggable.NULL_ADDRESS

    override fun getMutableCopy() = this

    override fun put(key: ByteIterable, value: ByteIterable): Boolean {
        val it = key.iterator()
        var node: MutableNode = root
        var prev: MutableNode? = null
        var prevFirstByte = 0.toByte()
        while (true) {
            val matchResult = node.matchesKeySequence(it)
            val matchingLength = NodeBase.MatchResult.getMatchingLength(matchResult)
            if (matchingLength < 0) {
                val prefix = node.splitKey(-matchingLength - 1, NodeBase.MatchResult.getKeyByte(matchResult))
                if (NodeBase.MatchResult.hasNext(matchResult)) {
                    prefix.hang(NodeBase.MatchResult.getNextByte(matchResult), it).setValue(value)
                } else {
                    prefix.setValue(value)
                }
                if (prev == null) {
                    root = MutableRoot(prefix, root.sourceAddress)
                } else {
                    prev.setChild(prevFirstByte, prefix)
                }
                ++size
                return true
            }
            if (!it.hasNext()) {
                val oldValue = node.getValue()
                node.setValue(value)
                if (oldValue == null) {
                    ++size
                    return true
                }
                return oldValue != value
            }
            val nextByte = it.next()
            val child = node.getChild(this, nextByte)
            if (child == null) {
                if (node.hasChildren() || node.hasKey() || node.hasValue()) {
                    node.hang(nextByte, it).setValue(value)
                } else {
                    node.setKeySequence(ArrayByteIterable(nextByte, it))
                    node.setValue(value)
                }
                ++size
                return true
            }
            prev = node
            prevFirstByte = nextByte
            val mutableChild = child.getMutableCopy(this)
            if (!child.isMutable) {
                node.setChild(nextByte, mutableChild)
            }
            node = mutableChild
        }
    }

    override fun putRight(key: ByteIterable, value: ByteIterable) {
        val it = key.iterator()
        var node: MutableNode = root
        var prev: MutableNode? = null
        var prevFirstByte = 0.toByte()
        while (true) {
            val matchResult = node.matchesKeySequence(it)
            val matchingLength = NodeBase.MatchResult.getMatchingLength(matchResult)
            if (matchingLength < 0) {
                require(NodeBase.MatchResult.hasNext(matchResult))
                val prefix = node.splitKey(-matchingLength - 1, NodeBase.MatchResult.getKeyByte(matchResult))
                prefix.hangRight(NodeBase.MatchResult.getNextByte(matchResult), it).setValue(value)
                if (prev == null) {
                    root = MutableRoot(prefix, root.sourceAddress)
                } else {
                    prev.setChild(prevFirstByte, prefix)
                }
                ++size
                break
            }
            if (!it.hasNext()) {
                require(!(node.hasChildren() || node.hasValue()))
                node.setValue(value)
                ++size
                break
            }
            val nextByte = it.next()
            val child = node.getRightChild(this, nextByte)
            if (child == null) {
                if (node.hasChildren() || node.hasKey() || node.hasValue()) {
                    node.hangRight(nextByte, it).setValue(value)
                } else {
                    node.setKeySequence(ArrayByteIterable(nextByte, it))
                    node.setValue(value)
                }
                ++size
                break
            }
            prev = node
            prevFirstByte = nextByte
            val mutableChild = child.getMutableCopy(this)
            if (!child.isMutable) {
                node.setRightChild(nextByte, mutableChild)
            }
            node = mutableChild
        }
    }

    override fun add(key: ByteIterable, value: ByteIterable): Boolean {
        val it = key.iterator()
        var node: NodeBase = root
        var mutableNode: MutableNode? = null
        val stack = ArrayDeque<ChildReferenceTransient>()
        while (true) {
            val matchResult = node.matchesKeySequence(it)
            val matchingLength = NodeBase.MatchResult.getMatchingLength(matchResult)
            if (matchingLength < 0) {
                val prefix = node.getMutableCopy(this)
                    .splitKey(-matchingLength - 1, NodeBase.MatchResult.getKeyByte(matchResult))
                if (NodeBase.MatchResult.hasNext(matchResult)) {
                    prefix.hang(NodeBase.MatchResult.getNextByte(matchResult), it).setValue(value)
                } else {
                    prefix.setValue(value)
                }
                if (stack.isEmpty()) {
                    root = MutableRoot(prefix, root.sourceAddress)
                } else {
                    val parent = stack.pop()
                    mutableNode = parent.mutate(this)
                    mutableNode.setChild(parent.firstByte, prefix)
                }
                break
            }
            if (!it.hasNext()) {
                if (node.hasValue()) {
                    return false
                }
                mutableNode = node.getMutableCopy(this)
                mutableNode.setValue(value)
                break
            }
            val nextByte = it.next()
            val child = node.getChild(this, nextByte)
            if (child == null) {
                mutableNode = node.getMutableCopy(this)
                if (mutableNode.hasChildren() || mutableNode.hasKey() || mutableNode.hasValue()) {
                    mutableNode.hang(nextByte, it).setValue(value)
                } else {
                    mutableNode.setKeySequence(ArrayByteIterable(nextByte, it))
                    mutableNode.setValue(value)
                }
                break
            }
            stack.push(ChildReferenceTransient(nextByte, node))
            node = child
        }
        ++size
        mutableNode?.let { mutateUp(stack, mutableNode) }
        return true
    }

    override fun delete(key: ByteIterable) = deleteImpl(key)

    override fun delete(
        key: ByteIterable,
        value: ByteIterable?,
        cursorToSkip: ITreeCursorMutable?
    ): Boolean {
        if (value == null) {
            if (deleteImpl(key)) {
                TreeCursorMutable.notifyCursors(this, cursorToSkip)
                return true
            }
            return false
        }
        throw UnsupportedOperationException("Patricia tree doesn't support duplicates!")
    }

    override fun put(ln: INode) {
        put(ln.key, getNotNullValue(ln))
    }

    override fun putRight(ln: INode) = putRight(ln.key, getNotNullValue(ln))

    override fun add(ln: INode) = add(ln.key, getNotNullValue(ln))

    override fun save(): Long {
        val context = MutableNodeSaveContext(CompressedUnsignedLongByteIterable.getIterable(size))
        val stack = ArrayDeque<ChildReferenceMutable>().apply {
            push(ChildReferenceMutable(root))
        }
        while (true) {
            val ref = stack.peek()
            val node = ref.child
            var childPushed = false
            val children = if (node.hasChildren()) node.getChildren().asSequence().filterNotNull() else emptySequence()
            for (r in children) {
                if (r.isMutable && r.suffixAddress == Loggable.NULL_ADDRESS) {
                    childPushed = true
                    stack.push(r as ChildReferenceMutable)
                }
            }
            if (childPushed) continue
            val savedAddress = node.save(this, context)
            stack.pop()
            if (stack.isEmpty()) return savedAddress
            ref.suffixAddress = savedAddress
        }
    }

    override fun getExpiredLoggables() = expiredLoggables ?: EMPTY

    override fun openCursor() =
        TreeCursorMutable(this, PatriciaTraverser(this, root), root.hasValue()).also { result ->
            (openCursors ?: HashSet<ITreeCursorMutable>().also { openCursors = it }).add(result)
        }

    override fun cursorClosed(cursor: ITreeCursorMutable) {
        openCursors.notNull.remove(cursor)
    }

    override fun reclaim(
            loggable: RandomAccessLoggable,
            loggables: Iterator<RandomAccessLoggable>,
            segmentSize: Long
    ): Boolean {
        var l = loggable
        var minAddress = l.address
        while (true) {
            val type = l.type
            if (type < NODE_WO_KEY_WO_VALUE_WO_CHILDREN || type > MAX_VALID_LOGGABLE_TYPE) {
                if (type != NullLoggable.TYPE) { // skip null loggable
                    throw ExodusException("Unexpected loggable type " + l.type)
                }
            } else {
                if (l.structureId != structureId) {
                    throw ExodusException("Unexpected structure id " + l.structureId)
                }
                if (nodeIsRoot(type)) {
                    break
                }
            }
            if (!loggables.hasNext()) {
                return false
            }
            l = loggables.next()
        }
        val maxAddress = l.address
        val sourceTree = PatriciaTreeForReclaim(log, maxAddress, structureId)
        val sourceRoot = sourceTree.root
        val backRef = sourceTree.backRef
        if (backRef > 0) {
            val treeStartAddress = sourceTree.rootAddress - backRef
            check(treeStartAddress <= minAddress) { "Wrong back reference!" }
            if (!log.hasAddressRange(treeStartAddress, maxAddress)) {
                return false
            }
            minAddress = treeStartAddress
        }
        val actual = PatriciaReclaimActualTraverser(this)
        reclaim(PatriciaReclaimSourceTraverser(sourceTree, sourceRoot, minAddress), actual)
        return actual.wasReclaim || sourceRoot.address == root.sourceAddress
    }

    override fun getRoot() = root

    override fun isAllowingDuplicates() = false

    override fun getOpenCursors() = openCursors

    fun mutateNode(node: ImmutableNode): MutableNode {
        addExpiredLoggable(node.loggable)
        return MutableNode(node)
    }

    private fun addExpiredLoggable(sourceLoggable: RandomAccessLoggable?) {
        if (sourceLoggable != null && sourceLoggable.address != NullLoggable.NULL_ADDRESS) {
            var expiredLoggables = expiredLoggables
            if (expiredLoggables == null) {
                expiredLoggables = ExpiredLoggableCollection()
                this.expiredLoggables = expiredLoggables
            }
            expiredLoggables.add(sourceLoggable)
        }
    }

    private fun deleteImpl(key: ByteIterable): Boolean {
        val it = key.iterator()
        var node: NodeBase? = root
        val stack: Deque<ChildReferenceTransient> = ArrayDeque()
        while (true) {
            if (node == null || NodeBase.MatchResult.getMatchingLength(node.matchesKeySequence(it)) < 0) {
                return false
            }
            if (!it.hasNext()) {
                break
            }
            val nextByte = it.next()
            stack.push(ChildReferenceTransient(nextByte, node))
            node = node.getChild(this, nextByte)
        }
        if (!checkNotNull(node).hasValue()) {
            return false
        }
        --size
        var mutableNode = node.getMutableCopy(this)
        val parent = stack.peek()
        val hasChildren = mutableNode.hasChildren()
        if (!hasChildren && parent != null) {
            stack.pop()
            mutableNode = parent.mutate(this)
            mutableNode.removeChild(parent.firstByte)
            if (!mutableNode.hasValue() && mutableNode.childrenCount == 1) {
                mutableNode.mergeWithSingleChild(this)
            }
        } else {
            mutableNode.setValue(null)
            if (!hasChildren) {
                mutableNode.setKeySequence(ByteIterable.EMPTY)
            } else if (mutableNode.childrenCount == 1) {
                mutableNode.mergeWithSingleChild(this)
            }
        }
        mutateUp(stack, mutableNode)
        return true
    }

    /*
     * stack contains all ancestors of the node, stack.peek() is its parent.
     */
    private fun mutateUp(stack: Deque<ChildReferenceTransient>, node: MutableNode) {
        var n = node
        while (!stack.isEmpty()) {
            val parent = stack.pop()
            val mutableParent = parent.mutate(this)
            mutableParent.setChild(parent.firstByte, n)
            n = mutableParent
        }
    }

    companion object {

        @JvmStatic
        fun getNotNullValue(ln: INode): ByteIterable {
            return ln.value ?: throw ExodusException("Value can't be null")
        }

        private fun reclaim(
            source: PatriciaReclaimSourceTraverser,
            actual: PatriciaReclaimActualTraverser
        ) {
            if (source.matches(actual)) {
                reclaimActualChildren(source, actual)
            } else {
                val stack = ArrayDeque<ReclaimFrame>()
                dive_deeper@
                while (true) {
                    var srcItr = source.currentNode.keySequence.iterator()
                    var actItr = actual.currentNode.keySequence.iterator()
                    var frame = ReclaimFrame()
                    var iteratorsMatch = false
                    while (true) {
                        if (srcItr.hasNext()) {
                            if (actItr.hasNext()) {
                                if (srcItr.next() != actItr.next()) { // key is not matching
                                    break
                                }
                            } else {
                                val children = actual.currentNode.getChildren(srcItr.next())
                                val child = children.node ?: break
                                actual.currentChild = child
                                actual.currentIterator = children
                                actual.moveDown()
                                frame.actPushes++
                                actItr = actual.currentNode.keySequence.iterator()
                            }
                        } else if (actItr.hasNext()) {
                            val children = source.currentNode.getChildren(actItr.next())
                            val child = children.node
                            if (child == null || !source.isAddressReclaimable(child.suffixAddress)) {
                                break // child can be expired if source parent was already not-current
                            }
                            source.currentChild = child
                            source.currentIterator = children
                            source.moveDown()
                            frame.srcPushes++
                            srcItr = source.currentNode.keySequence.iterator()
                        } else {
                            // both iterators matched, here comes the branching
                            source.moveToNextReclaimable()
                            iteratorsMatch = true
                            break
                        }
                    }
                    while (true) {
                        if (iteratorsMatch) {
                            while (source.isValidPos && actual.isValidPos) {
                                val sourceChild = source.currentChild
                                val sourceByte = sourceChild.firstByte.toInt() and 0xff
                                val actualByte = actual.currentChild.firstByte.toInt() and 0xff
                                if (sourceByte < actualByte) {
                                    source.moveRight()
                                } else if (sourceByte > actualByte) {
                                    actual.moveRight()
                                } else {
                                    source.moveDown()
                                    actual.moveDown()
                                    if (source.matches(actual)) {
                                        reclaimActualChildren(source, actual)
                                        upAndRight(source, actual)
                                    } else {
                                        stack.push(frame)
                                        continue@dive_deeper
                                    }
                                }
                            }
                        }
                        repeat(frame.srcPushes) { source.moveUp() }
                        repeat(frame.actPushes) { actual.popAndMutate() }
                        frame = stack.poll() ?: break@dive_deeper
                        upAndRight(source, actual)
                        iteratorsMatch = true
                    }
                }
            }
        }

        private fun PatriciaReclaimSourceTraverser.matches(actual: PatriciaReclaimActualTraverser) =
            currentNode.address == actual.currentNode.address

        private fun reclaimActualChildren(
            source: PatriciaReclaimSourceTraverser,
            actual: PatriciaReclaimActualTraverser
        ) {
            actual.currentNode = actual.currentNode.getMutableCopy(actual.mainTree)
            actual.getItr()
            actual.wasReclaim = true
            var depth = 1
            dive_deeper@
            while (true) {
                while (actual.isValidPos) {
                    val actualChild = actual.currentChild
                    val suffixAddress = actualChild.suffixAddress
                    if (source.isAddressReclaimable(suffixAddress)) {
                        actual.moveDown()
                        actual.currentNode = actual.currentNode.getMutableCopy(actual.mainTree)
                        actual.getItr()
                        actual.wasReclaim = true
                        depth++
                        continue@dive_deeper
                    }
                    actual.moveRight()
                }
                if (--depth == 0) {
                    break
                }
                actual.popAndMutate()
                actual.moveRight()
            }
        }

        private fun upAndRight(
            source: PatriciaReclaimSourceTraverser,
            actual: PatriciaReclaimActualTraverser
        ) {
            actual.popAndMutate()
            source.moveUp()
            source.moveRight()
            actual.moveRight()
        }

        private class ReclaimFrame {
            var srcPushes = 0
            var actPushes = 0
        }
    }
}

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
package jetbrains.exodus.query

import jetbrains.exodus.ExodusException
import org.slf4j.LoggerFactory
import java.lang.Integer.max

@Suppress("LeakingThis")
abstract class BinaryOperator internal constructor(private var left: NodeBase, private var right: NodeBase) : NodeBase() {

    private var children: MutableList<NodeBase>? = null
    protected var depth: Int = 0

    init {
        this.left = NodeBase.getUnderRoot(left)
        this.right = NodeBase.getUnderRoot(right)
        this.left.parent = this
        this.right.parent = this
        invalidateDepth(left, right)
        if (isWarnEnabled && depth >= MAXIMUM_LEGAL_DEPTH && depth % MAXIMUM_LEGAL_DEPTH == 0) {
            val millis = System.currentTimeMillis()
            if (millis - lastLoggedMillis > LARGE_DEPTH_LOGGING_FREQ) {
                lastLoggedMillis = millis
                logger.warn("Binary operator of too great depth", Throwable())
            }
        }
    }

    fun getLeft() = left

    fun getRight() = right

    override fun replaceChild(child: NodeBase, newChild: NodeBase): NodeBase {
        when {
            child === left -> setLeft(newChild)
            child === right -> setRight(newChild)
            else -> throw ExodusException(javaClass.toString() + ": can't replace not own child")
        }
        newChild.parent = this
        return child
    }

    override fun getChildren(): Collection<NodeBase> {
        return children ?: ArrayList<NodeBase>(2).apply {
            add(left)
            add(right)
            children = this
        }
    }

    internal override fun matchChildren(node: NodeBase, ctx: NodeBase.MatchContext): Boolean {
        val bo = node as BinaryOperator
        return left.match(bo.left, ctx) && right.match(bo.right, ctx)
    }

    protected fun setLeft(left: NodeBase) {
        this.left = left
        children?.also { it[0] = left }
        invalidateDepth(left, right)
    }

    protected fun setRight(right: NodeBase) {
        this.right = right
        children?.also { it[1] = right }
        invalidateDepth(left, right)
    }

    private fun invalidateDepth(left: NodeBase, right: NodeBase) {
        val leftDepth = (left as? BinaryOperator)?.depth ?: 1
        val rightDepth = (right as? BinaryOperator)?.depth ?: 1
        depth = max(leftDepth, rightDepth) + 1
    }

    override fun getHandle(sb: StringBuilder): StringBuilder {
        super.getHandle(sb).append('{')
        return right.getHandle(left.getHandle(sb).append(',')).append('}')
    }

    companion object {

        private val logger = LoggerFactory.getLogger(BinaryOperator::class.java)
        private val isWarnEnabled = logger.isWarnEnabled
        private const val MAXIMUM_LEGAL_DEPTH = 200
        private const val LARGE_DEPTH_LOGGING_FREQ = 10000
        @Volatile
        private var lastLoggedMillis: Long = 0
    }
}

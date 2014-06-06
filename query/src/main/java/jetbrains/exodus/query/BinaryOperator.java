/**
 * Copyright 2010 - 2014 JetBrains s.r.o.
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
package jetbrains.exodus.query;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public abstract class BinaryOperator extends NodeBase {
    private static final int MAXIMUM_LEGAL_DEPTH = 200;
    protected static Log log = LogFactory.getLog(BinaryOperator.class);

    private NodeBase left;
    private NodeBase right;
    private List<NodeBase> children;
    private final int depth;

    BinaryOperator(@NotNull final NodeBase left, @NotNull final NodeBase right) {
        this.left = getUnderRoot(left);
        this.right = getUnderRoot(right);
        this.left.setParent(this);
        this.right.setParent(this);
        final int leftDepth = left instanceof BinaryOperator ?
                ((BinaryOperator) left).depth :
                1;
        final int rightDepth = right instanceof BinaryOperator ?
                ((BinaryOperator) right).depth :
                1;
        depth = leftDepth < rightDepth ?
                rightDepth + 1 :
                leftDepth + 1;
        if (depth >= MAXIMUM_LEGAL_DEPTH && (depth % 100) == 0) {
            if (log.isWarnEnabled()) {
                log.warn("Binary operator of too great depth", new Throwable());
            }
        }
    }

    public NodeBase getLeft() {
        return left;
    }

    public NodeBase getRight() {
        return right;
    }

    @Override
    public NodeBase replaceChild(final NodeBase child, final NodeBase newChild) {
        if (child == left) {
            setLeft(newChild);
        } else if (child == right) {
            setRight(newChild);
        } else {
            throw new RuntimeException(getClass() + ": can't replace not own child");
        }
        newChild.setParent(this);
        return child;
    }

    @Override
    public Collection<NodeBase> getChildren() {
        if (children == null) {
            final ArrayList<NodeBase> result = new ArrayList<NodeBase>(2);
            result.add(left);
            result.add(right);
            children = result;
        }
        return children;
    }

    @Override
    protected boolean matchChildren(NodeBase node, NodeBase.MatchContext ctx) {
        final BinaryOperator _node = (BinaryOperator) node;
        if (!(left.match(_node.left, ctx))) {
            return false;
        }
        return right.match(_node.right, ctx);
    }

    void setLeft(NodeBase left) {
        this.left = left;
        if (children != null) {
            children.set(0, left);
        }
    }

    void setRight(NodeBase right) {
        this.right = right;
        if (children != null) {
            children.set(1, right);
        }
    }

    /*package*/
    @Override
    public StringBuilder getHandle(StringBuilder sb) {
        super.getHandle(sb).append('{');
        sb = left.getHandle(sb).append(',');
        return right.getHandle(sb).append('}');
    }
}

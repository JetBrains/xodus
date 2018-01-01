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
package jetbrains.exodus.query;


import jetbrains.exodus.core.dataStructures.NanoSet;

import java.util.Collection;

public abstract class UnaryNode extends NodeBase {
    protected NodeBase child;
    private Collection<NodeBase> children;

    protected UnaryNode(final NodeBase child) {
        this.child = getUnderRoot(child);
        this.child.setParent(this);
    }

    public NodeBase getChild() {
        return child;
    }

    @Override
    public NodeBase replaceChild(NodeBase child, NodeBase newChild) {
        if (this.child != child) {
            throw new RuntimeException(getClass() + ": can't replace not own child.");
        }
        this.child = newChild;
        children = null;
        newChild.setParent(this);
        return child;
    }

    @Override
    public Collection<NodeBase> getChildren() {
        if (children == null) {
            children = new NanoSet<>(child);
        }
        return children;
    }

    @Override
    public StringBuilder getHandle(StringBuilder sb) {
        super.getHandle(sb).append('{');
        return child.getHandle(sb).append('}');
    }

    @Override
    protected boolean matchChildren(NodeBase node, NodeBase.MatchContext ctx) {
        return child.match(((UnaryNode) node).child, ctx);
    }
}

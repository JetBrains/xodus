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


import jetbrains.exodus.entitystore.Entity;
import jetbrains.exodus.query.metadata.ModelMetaData;
import jetbrains.exodus.util.StringInterner;

import java.util.*;

@SuppressWarnings("HardcodedLineSeparator")
public abstract class NodeBase {
    protected static final List<NodeBase> NO_CHILDREN = Collections.emptyList();
    static final String TREE_LEVEL_INDENT = "  ";

    private NodeBase parent;

    protected NodeBase() {
    }

    public NodeBase getParent() {
        return parent;
    }

    public void setParent(NodeBase parent) {
        if (this.parent != null) {
            this.parent = null;
        }
        this.parent = parent;
    }

    public NodeBase replaceChild(NodeBase child, NodeBase newChild) {
        throw new RuntimeException(getClass() + ": can't replace child.");
    }

    public abstract Iterable<Entity> instantiate(String entityType, QueryEngine queryEngine, ModelMetaData metaData);

    public abstract NodeBase getClone();

    public Collection<NodeBase> getChildren() {
        return NO_CHILDREN;
    }

    public void optimize(Sorts sorts, OptimizationPlan rules) {
        boolean applied = true;
        while (applied) {
            applied = false;
            for (final NodeBase child : getChildren()) {
                if (!rules.applyOnEnter) {
                    child.optimize(sorts, rules);
                }
                for (OptimizationRule rule : rules.rules) {
                    if (child.replaceIfMatches(rule)) {
                        applied = true;
                        break;
                    }
                }
                if (applied) {
                    break;
                }
                if (rules.applyOnEnter) {
                    child.optimize(sorts, rules);
                }
            }
        }
    }

    public void cleanSorts(Sorts sorts) {
        for (NodeBase child : getChildren()) {
            child.cleanSorts(sorts);
        }
    }

    @SuppressWarnings({"EqualsWhichDoesntCheckParameterClass"})
    @Override
    public boolean equals(Object obj) {
        NodeBase node = (NodeBase) obj;
        Iterator<NodeBase> iterator = node.getChildren().iterator();
        for (NodeBase child1 : getChildren()) {
            NodeBase child2 = iterator.next();
            if (!(child1.equals(child2))) {
                return false;
            }
        }
        return true;
    }

    public void checkWildcard(Object obj) {
        if (obj instanceof Wildcard || obj instanceof ConversionWildcard) {
            throw new RuntimeException("Can't compare wildcard with " + obj.getClass() + '.');
        }
    }

    boolean replaceIfMatches(OptimizationRule rule) {
        NodeBase.MatchContext ctx = new NodeBase.MatchContext();
        if (!(match(rule.getSource(), ctx))) {
            return false;
        }
        parent.replaceChild(this, substituteMatches(rule.getDest(), ctx));
        return true;
    }

    protected boolean match(NodeBase node, NodeBase.MatchContext ctx) {
        if (node instanceof Wildcard) {
            final NodeBase nodeInst = ctx.getNode((Wildcard) node);
            if (nodeInst == null) {
                ctx.putNode((Wildcard) node, this);
                return true;
            }
            return equals(nodeInst);
        }
        if (node instanceof ConversionWildcard) {
            ConversionWildcard customWildcard = (ConversionWildcard) node;
            if (!(getClass().equals(customWildcard.getClazz()))) {
                return false;
            }
            if (!customWildcard.isOk(this)) {
                return false;
            }
            NodeBase leafInst = ctx.getLeave((ConversionWildcard) node);
            if (leafInst == null) {
                ctx.putLeave(customWildcard, this);
                return true;
            }
            return equals(leafInst);
        }
        return getClass().equals(node.getClass()) && matchChildren(node, ctx);
    }

    protected boolean polymorphic() {
        return false;
    }

    protected boolean matchChildren(NodeBase node, NodeBase.MatchContext ctx) {
        return true;
    }

    public String toString() {
        return toString("");
    }

    boolean toString(StringBuilder result, NodeBase subtree, String presentation) {
        return toString(result, "", subtree, presentation);
    }

    protected String toString(String prefix) {
        final StringBuilder result = new StringBuilder(prefix).append(getClass().getSimpleName());
        for (NodeBase child : getChildren()) {
            result.append('\n').append(child.toString(TREE_LEVEL_INDENT + prefix));
        }
        return result.toString();
    }

    private boolean toString(StringBuilder result, String prefix, NodeBase subtree, String presentation) {
        if (equals(subtree)) {
            result.append((prefix + presentation).replace("\n", '\n' + prefix));
            return true;
        }
        result.append(prefix);
        result.append(getClass().getSimpleName());
        boolean used = false;
        for (NodeBase child : getChildren()) {
            result.append('\n');
            StringBuilder childResult = new StringBuilder();
            boolean presentationUsed = !used && child.toString(childResult, TREE_LEVEL_INDENT + prefix, subtree, presentation);
            if (presentationUsed) {
                subtree = null;
                result.append(childResult);
            } else {
                result.append(child.toString(TREE_LEVEL_INDENT + prefix));
            }
            used |= presentationUsed;
        }
        return used;
    }

    String getHandle() {
        return StringInterner.intern(getHandle(new StringBuilder(32)), 100);
    }

    public StringBuilder getHandle(StringBuilder sb) {
        return sb.append(getSimpleName());
    }

    public abstract String getSimpleName();

    public int size() {
        int r = 1;
        for (NodeBase child : getChildren()) {
            r += child.size();
        }
        return r;
    }

    public Iterable<NodeBase> getDescendants() {
        return new Iterable<NodeBase>() {
            @Override
            public Iterator<NodeBase> iterator() {
                final List<NodeBase> stack = new LinkedList<>();
                stack.add(NodeBase.this); // push root
                return new Iterator<NodeBase>() {
                    @Override
                    public boolean hasNext() {
                        return !stack.isEmpty();
                    }

                    @Override
                    public NodeBase next() {
                        if (stack.isEmpty()) {
                            throw new NoSuchElementException();
                        } else {
                            final NodeBase result = stack.remove(0); // pop
                            stack.addAll(0, result.getChildren()); // push all children (first will be on top)
                            return result;
                        }
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    public static NodeBase getUnderRoot(NodeBase root) {
        while (root instanceof Root) {
            root = ((UnaryNode) root).getChild();
        }
        return root;
    }

    private static NodeBase substituteMatches(NodeBase pattern, NodeBase.MatchContext ctx) {
        Root root = new Root(pattern.getClone());
        for (NodeBase node : root.getDescendants()) {
            if (node instanceof Wildcard) {
                node.parent.replaceChild(node, ctx.getNode((Wildcard) node));
            } else if (node instanceof ConversionWildcard) {
                ConversionWildcard customWildcard = (ConversionWildcard) node;
                node.parent.replaceChild(node, customWildcard.convert(ctx.getLeave((ConversionWildcard) node)));
            }
        }
        return root.getChild();
    }

    static class MatchContext {
        private Map<Wildcard, NodeBase> nodes;
        private Map<ConversionWildcard, NodeBase> leaves;

        private MatchContext() {
        }

        private NodeBase getNode(Wildcard wildcard) {
            return nodes == null ?
                    null :
                    nodes.get(wildcard);
        }

        private void putNode(Wildcard wildcard, NodeBase node) {
            if (nodes == null) {
                nodes = new HashMap<>();
            }
            nodes.put(wildcard, node);
        }

        private NodeBase getLeave(ConversionWildcard wildcard) {
            return leaves == null ?
                    null :
                    leaves.get(wildcard);
        }

        private void putLeave(ConversionWildcard wildcard, NodeBase node) {
            if (leaves == null) {
                leaves = new HashMap<>();
            }
            leaves.put(wildcard, node);
        }
    }
}

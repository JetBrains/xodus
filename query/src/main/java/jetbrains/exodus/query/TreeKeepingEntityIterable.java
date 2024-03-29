/*
 * Copyright ${inceptionYear} - ${year} ${owner}
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
package jetbrains.exodus.query;


import jetbrains.exodus.entitystore.DualCompatibilityKt;
import jetbrains.exodus.entitystore.Entity;
import jetbrains.exodus.entitystore.Explainer;
import jetbrains.exodus.entitystore.PersistentEntityStoreImpl;
import jetbrains.exodus.entitystore.iterate.EntityIterableBase;
import jetbrains.exodus.query.metadata.EntityMetaData;
import jetbrains.exodus.query.metadata.ModelMetaData;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"HardcodedLineSeparator", "AssignmentToMethodParameter", "ConstructorWithTooManyParameters"})
public class TreeKeepingEntityIterable extends StaticTypedEntityIterable {

    private static final Logger logger = LoggerFactory.getLogger(TreeKeepingEntityIterable.class);

    private final Iterable<Entity> instance;
    private final NodeBase sourceTree;
    private NodeBase optimizedTree;
    private Sorts sorts;
    String annotatedTree;

    public TreeKeepingEntityIterable(@Nullable final Iterable<Entity> entityIterable, @NotNull String entityType,
                                     @NotNull final NodeBase queryTree, @Nullable String leftChildPresentation,
                                     @Nullable String rightChildPresentation, @NotNull QueryEngine queryEngine) {
        super(queryEngine);
        // get entityType from iterable
        if (entityIterable instanceof StaticTypedEntityIterable) {
            final String entityIterableType = ((StaticTypedEntityIterable) entityIterable).getEntityType();
            if (!(entityType.equals(entityIterableType)) && Utils.isTypeOf(entityIterableType, entityType,
                    queryEngine.getModelMetaData())) {
                entityType = entityIterableType;
            }
        }
        //
        if (entityIterable instanceof TreeKeepingEntityIterable) {
            final TreeKeepingEntityIterable instanceTreeIt = (TreeKeepingEntityIterable) entityIterable;
            final NodeBase instanceTree = instanceTreeIt.sourceTree;
            if (queryTree instanceof Sort && ((UnaryNode) queryTree).getChild().equals(NodeFactory.all())) {
                sourceTree = queryTree.getClone();
                sourceTree.replaceChild(((UnaryNode) sourceTree).getChild(), instanceTree.getClone());

            } else {
                sourceTree = instanceTree instanceof GetAll ? queryTree : And.and(instanceTree.getClone(), queryTree);
            }
            instance = instanceTreeIt.instance;
        } else {
            instance = entityIterable;
            sourceTree = queryTree;
        }
        this.entityType = entityType;
        optimizedTree = null;
    }

    public TreeKeepingEntityIterable(@Nullable final Iterable<Entity> entityIterable, @NotNull String entityType,
                                     @NotNull final NodeBase queryTree, @NotNull QueryEngine queryEngine) {
        this(entityIterable, entityType, queryTree, null, null, queryEngine);
    }

    @SuppressWarnings("UnusedDeclaration")
    @Override
    public Iterable<Entity> instantiate() {
        optimize();
        Iterable<Entity> result;
        if (instance == null) {
            result = instantiateForWholeHierarchy();
        } else if (optimizedTree instanceof GetAll) {
            result = instance;
        } else {
            // clone
            TreeKeepingEntityIterable tkei = new TreeKeepingEntityIterable(null, entityType, optimizedTree.getClone(), queryEngine);
            tkei.optimizedTree = optimizedTree;
            result = queryEngine.toEntityIterable(queryEngine.intersect(instance, tkei));
        }
        if (sorts != null) {
            result = sorts.apply(entityType, result, queryEngine);
        }
        if (result == null) {
            result = instantiateForWholeHierarchy();
        }
        return result;
    }

    private Iterable<Entity> instantiateForWholeHierarchy() {
        return instantiateForWholeHierarchy(entityType, optimizedTree);
    }

    private Iterable<Entity> instantiateForWholeHierarchy(final String entityType, final NodeBase ast) {
        final ModelMetaData mmd = queryEngine.getModelMetaData();
        @Nullable final EntityMetaData emd = mmd == null ? null : mmd.getEntityMetaData(entityType);
        Iterable<Entity> result = (emd != null && emd.isAbstract()) ?
            EntityIterableBase.EMPTY :
            ast.getClone().instantiate(entityType, queryEngine, mmd, new NodeBase.InstantiateContext());
        if (!(emd == null || ast.polymorphic())) {
            for (String subType : emd.getSubTypes()) {
                if (Utils.getUnionSubtypes()) {
                    // union returns sorted by id results provided its operands are sorted by id
                    result = queryEngine.unionAdjusted(result, instantiateForWholeHierarchy(subType, ast));
                } else {
                    result = queryEngine.concatAdjusted(result, instantiateForWholeHierarchy(subType, ast));
                }
            }
        }
        return queryEngine.adjustEntityIterable(result);
    }

    public void optimize() {
        if (optimizedTree == null) {
            final boolean sourceCanBeCached = sourceTree.canBeCached();
            OptimizedTreesCache.OptimizedTreeAndSorts optimized;
            if (sourceCanBeCached && (optimized = OptimizedTreesCache.get().findOptimized(sourceTree)) != null) {
                optimizedTree = optimized.getOptimizedTree();
                sorts = optimized.getSorts();
            } else {
                final long start = System.currentTimeMillis();
                sorts = new Sorts();
                final Root root = new Root(sourceTree.getClone());
                for (OptimizationPlan rules : OptimizationPlan.PLANS) {
                    root.optimize(sorts, rules);
                }
                root.cleanSorts(sorts);
                optimizedTree = root.getChild();
                if (sourceCanBeCached && sorts.canBeCached()) {
                    OptimizedTreesCache.get().cacheOptimized(sourceTree, optimizedTree, sorts);
                }
                final long delta = System.currentTimeMillis() - start;
                if (delta > 1) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Optimize tree in [" + delta + " ms]");
                    }
                    if (logger.isTraceEnabled()) {
                        logger.trace("---------------------------------------------------");
                        logger.trace("Source tree: ");
                        logger.trace(sourceTree.toString());
                        logger.trace("---------------------------------------------------");
                    }
                }
                if (logger.isTraceEnabled()) {
                    logger.trace("Optimized tree: ");
                    logger.trace(optimizedTree.toString());
                    logger.trace("---------------------------------------------------");
                }
            }
        }
    }

    public Iterable<Entity> getInstance() {
        return instance;
    }

    @Override
    public String getEntityType() {
        return entityType;
    }

    public NodeBase getTree() {
        return sourceTree;
    }

    public NodeBase getOptimizedTree() {
        return optimizedTree;
    }

    public Sorts getSorts() {
        return sorts;
    }
}
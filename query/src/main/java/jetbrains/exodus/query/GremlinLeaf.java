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


import jetbrains.exodus.core.dataStructures.NanoSet;
import jetbrains.exodus.entitystore.Entity;
import jetbrains.exodus.entitystore.youtrackdb.gremlin.GremlinEntityIterable;
import jetbrains.exodus.entitystore.youtrackdb.gremlin.GremlinEntityIterableImpl;
import jetbrains.exodus.entitystore.youtrackdb.gremlin.GremlinBlock;
import jetbrains.exodus.entitystore.youtrackdb.gremlin.GremlinQuery;
import jetbrains.exodus.query.metadata.ModelMetaData;

import javax.annotation.Nonnull;
import java.util.Objects;

public class GremlinLeaf extends NodeBase implements GremlinNode {

    private final GremlinBlock block;

    public GremlinLeaf(GremlinBlock query) {
        this.block = query;
    }

    @Override
    @Nonnull
    public GremlinBlock getBlock() {
        return block;
    }

    @Override
    public Iterable<Entity> instantiate(String entityType, QueryEngine queryEngine, ModelMetaData metaData, InstantiateContext context) {
        return GremlinEntityIterable.where(
                entityType,
                queryEngine.getOStore().requireActiveTransaction(),
                block
        );
    }

    @Override
    public NodeBase getClone() {
        return new GremlinLeaf(block);
    }

    @Override
    public String getSimpleName() {
        return block.getShortName();
    }

    @Override
    public int size() {
        return 1;
    }

    @Override
    public void optimize(Sorts sorts, OptimizationPlan rules) {
        final var simplified = block.simplify();
        if (simplified != null) {
            getParent().replaceChild(this, new GremlinLeaf(simplified));
        }
    }

    @Override
    public Iterable<NodeBase> getDescendants() {
        return new NanoSet<>(this);
    }

    @Override
    public StringBuilder getHandle(StringBuilder s) {
        // todo: think of this. if this needed at all
        return block.describe(s);
    }

    @Override
    public String toString() {
        return block.describe(new StringBuilder()).toString();
    }

    @Override
    public boolean equals(Object o) {
        // todo: checkWildcard
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        GremlinLeaf that = (GremlinLeaf) o;
        return Objects.equals(block, that.block);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(block);
    }
}

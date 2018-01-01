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

import jetbrains.exodus.core.dataStructures.SoftConcurrentObjectCache;
import jetbrains.exodus.core.dataStructures.SoftObjectCache;
import jetbrains.exodus.core.dataStructures.SoftObjectCacheBase;

public class OptimizedTreesCache {

    private static final OptimizedTreesCache INSTANCE = new OptimizedTreesCache();
    private static final int CACHE_SIZE = 16384;

    private final SoftObjectCacheBase<String, OptimizedTreeAndSorts> cache;

    private OptimizedTreesCache() {
        final boolean blocking = "false".equals(System.getProperty("exodus.query.treeCache.nonBlocking"));
        cache = blocking ?
                new SoftObjectCache<String, OptimizedTreeAndSorts>(CACHE_SIZE) :
                new SoftConcurrentObjectCache<String, OptimizedTreeAndSorts>(CACHE_SIZE);
    }

    OptimizedTreeAndSorts findOptimized(NodeBase tree) {
        return cache.tryKey(tree.getHandle());
    }

    void cacheOptimized(NodeBase original, NodeBase optimized, Sorts sorts) {
        cache.cacheObject(original.getHandle(), new OptimizedTreeAndSorts(optimized, sorts));
    }

    public static OptimizedTreesCache get() {
        return INSTANCE;
    }

    public static class OptimizedTreeAndSorts {
        private final Sorts sorts;
        private final NodeBase optimizedTree;

        public OptimizedTreeAndSorts(NodeBase optimizedTree, Sorts sorts) {
            this.optimizedTree = optimizedTree;
            this.sorts = sorts;
        }

        public Sorts getSorts() {
            return sorts;
        }

        public NodeBase getOptimizedTree() {
            return optimizedTree;
        }
    }
}

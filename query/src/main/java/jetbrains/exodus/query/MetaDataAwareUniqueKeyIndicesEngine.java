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

import jetbrains.exodus.core.dataStructures.hash.HashSet;
import jetbrains.exodus.entitystore.PersistentEntityStoreImpl;
import jetbrains.exodus.query.metadata.Index;
import jetbrains.exodus.query.metadata.ModelMetaData;
import org.jetbrains.annotations.NotNull;

import java.util.Set;

public class MetaDataAwareUniqueKeyIndicesEngine extends UniqueKeyIndicesEngine {
    private final ModelMetaData modelMetaData;

    MetaDataAwareUniqueKeyIndicesEngine(@NotNull final PersistentEntityStoreImpl persistentStore,
                                        final ModelMetaData modelMetaData) {
        super(persistentStore);
        this.modelMetaData = modelMetaData;
    }

    @Override
    protected Set<String> getEntityTypesToIndex(@NotNull Index index) {
        Set<String> res = new HashSet<>();
        final String entityType = index.getOwnerEntityType();
        res.add(entityType);
        res.addAll(modelMetaData.getEntityMetaData(entityType).getAllSubTypes());
        return res;
    }
}

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
package jetbrains.exodus.query.metadata;

import jetbrains.exodus.core.dataStructures.hash.HashMap;
import jetbrains.exodus.core.dataStructures.hash.HashSet;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ModelMetaDataImpl implements ModelMetaData {

    private static final Logger logger = LoggerFactory.getLogger(ModelMetaDataImpl.class);
    private static final boolean LOG_RESET = Boolean.getBoolean("jetbrains.exodus.query.metadata.logReset");

    private final Set<EntityMetaData> entityMetaDatas = Collections.newSetFromMap(new ConcurrentHashMap<EntityMetaData, Boolean>());
    private final Map<String, AssociationMetaData> associationMetaDatas = new ConcurrentHashMap<>();
    private volatile Map<String, EntityMetaData> typeToEntityMetaDatas = null;

    public void init() {
        reset();
        prepare();
    }

    public void setEntityMetaDatas(@NotNull Set<EntityMetaData> entityMetaDatas) {
        this.entityMetaDatas.clear();
        this.entityMetaDatas.addAll(entityMetaDatas);
        for (EntityMetaData emd : entityMetaDatas) {
            ((EntityMetaDataImpl) emd).setModelMetaData(this);
        }
    }

    public void setAssociationMetaDatas(Set<AssociationMetaData> associationMetaDatas) {
        for (AssociationMetaData amd : associationMetaDatas) {
            this.associationMetaDatas.put(((AssociationMetaDataImpl) amd).getFullName(), amd);
        }
    }

    public void addEntityMetaData(@NotNull EntityMetaData emd) {
        entityMetaDatas.add(emd);
        ((EntityMetaDataImpl) emd).setModelMetaData(this);
        reset();
    }

    void reset() {
        if (LOG_RESET) {
            logger.info("ModelMetaDataImpl#reset() invoked in thread " + Thread.currentThread(), new Throwable());
        }
        synchronized (entityMetaDatas) {
            typeToEntityMetaDatas = null;
        }
    }

    @NotNull
    public Map<String, EntityMetaData> prepare() {
        Map<String, EntityMetaData> result = typeToEntityMetaDatas;
        if (result != null) {
            return result;
        }

        synchronized (entityMetaDatas) {
            result = typeToEntityMetaDatas;
            if (result != null) {
                return result;
            }
            result = new HashMap<>();

            for (final EntityMetaData emd : entityMetaDatas) {
                ((EntityMetaDataImpl) emd).reset();

                final String type = emd.getType();
                if (result.get(type) != null) {
                    throw new IllegalArgumentException("Duplicate entity [" + type + ']');
                }
                result.put(type, emd);
            }

            this.typeToEntityMetaDatas = result;

            for (EntityMetaData emd : entityMetaDatas) {
                final EntityMetaDataImpl impl = (EntityMetaDataImpl) emd;
                final Set<AssociationEndMetaData> ends = impl.getExternalAssociationEnds();
                if (ends != null) {
                    for (AssociationEndMetaData aemd : ends) {
                        final AssociationEndMetaDataImpl endImpl = (AssociationEndMetaDataImpl) aemd;
                        endImpl.resolve(this, associationMetaDatas.get(endImpl.getAssociationMetaDataName()));
                    }
                }
            }

            for (final EntityMetaData emd : entityMetaDatas) {
                Set<AssociationEndMetaData> ends = ((EntityMetaDataImpl) emd).getExternalAssociationEnds();
                final boolean wasNull = ends == null;
                String superType = emd.getSuperType();
                while (superType != null) {
                    EntityMetaData parent = result.get(superType);
                    Set<AssociationEndMetaData> parentEnds = ((EntityMetaDataImpl) parent).getExternalAssociationEnds();
                    if (parentEnds != null) {
                        if (ends == null) {
                            ends = new HashSet<>(parentEnds);
                        } else {
                            ends.addAll(parentEnds);
                        }
                    }
                    superType = parent.getSuperType();
                }
                if (wasNull && ends != null) {
                    // non-null ends are mutated in-place
                    ((EntityMetaDataImpl) emd).setAssociationEnds(ends);
                }
            }

            for (final EntityMetaData emd : entityMetaDatas) {
                // add subtype
                final String superType = emd.getSuperType();
                if (superType != null) {
                    addSubTypeToMetaData(result, emd, superType);
                }
                // add interface types
                for (String iFaceType : emd.getInterfaceTypes()) {
                    addSubTypeToMetaData(result, emd, iFaceType);
                }

                // set supertypes
                List<String> thisAndSuperTypes = new ArrayList<>();
                EntityMetaData data = emd;
                String t = data.getType();
                do {
                    thisAndSuperTypes.add(t);
                    thisAndSuperTypes.addAll(data.getInterfaceTypes());
                    data = result.get(t);
                    t = data.getSuperType();
                } while (t != null);
                ((EntityMetaDataImpl) emd).setThisAndSuperTypes(thisAndSuperTypes);
            }
            return result;
        }
    }

    private void addSubTypeToMetaData(Map<String, EntityMetaData> typeToEntityMetaDatas, EntityMetaData emd, String superType) {
        final EntityMetaData superEmd = typeToEntityMetaDatas.get(superType);
        if (superEmd == null) {
            throw new IllegalArgumentException("No entity metadata for super type [" + superType + "]");
        }
        ((EntityMetaDataImpl) superEmd).addSubType(emd.getType());
    }

    @Override
    @Nullable
    public EntityMetaData getEntityMetaData(@NotNull String entityType) {
        return prepare().get(entityType);
    }

    @Override
    @NotNull
    public Iterable<EntityMetaData> getEntitiesMetaData() {
        return prepare().values();
    }

    public boolean hasAssociation(String sourceEntityName, String targetEntityName, String sourceName) {
        return associationMetaDatas.containsKey(getUniqueAssociationName(sourceEntityName, targetEntityName, sourceName));
    }

    @Override
    public AssociationMetaData addAssociation(String sourceEntityName, String targetEntityName,
                                              AssociationType type,
                                              String sourceName, AssociationEndCardinality sourceCardinality,
                                              boolean sourceCascadeDelete, boolean sourceClearOnDelete, boolean sourceTargetCascadeDelete, boolean sourceTargetClearOnDelete,
                                              String targetName, AssociationEndCardinality targetCardinality,
                                              boolean targetCascadeDelete, boolean targetClearOnDelete, boolean targetTargetCascadeDelete, boolean targetTargetClearOnDelete
    ) {

        EntityMetaDataImpl source = (EntityMetaDataImpl) getEntityMetaData(sourceEntityName);
        if (source == null) throw new IllegalArgumentException("Can't find entity " + sourceEntityName);

        EntityMetaDataImpl target = (EntityMetaDataImpl) getEntityMetaData(targetEntityName);
        if (target == null) throw new IllegalArgumentException("Can't find entity " + targetEntityName);


        AssociationEndType sourceType = null;
        AssociationEndType targetType = null;

        AssociationMetaDataImpl amd = new AssociationMetaDataImpl();
        amd.setType(type);
        String fullName = getUniqueAssociationName(sourceEntityName, targetEntityName, sourceName);
        amd.setFullName(fullName);
        associationMetaDatas.put(fullName, amd);

        switch (type) {
            case Directed:
                sourceType = AssociationEndType.DirectedAssociationEnd;
                break;

            case Undirected:
                sourceType = AssociationEndType.UndirectedAssociationEnd;
                targetType = AssociationEndType.UndirectedAssociationEnd;
                break;

            case Aggregation:
                sourceType = AssociationEndType.ParentEnd;
                targetType = AssociationEndType.ChildEnd;
                break;
        }

        AssociationEndMetaDataImpl sourceEnd = new AssociationEndMetaDataImpl(
            amd, sourceName, target, sourceCardinality, sourceType,
            sourceCascadeDelete, sourceClearOnDelete, sourceTargetCascadeDelete, sourceTargetClearOnDelete);
        addAssociationEndMetaDataToEntityTypeSubtree(prepare(), source, sourceEnd);

        if (type != AssociationType.Directed) {
            AssociationEndMetaDataImpl targetEnd = new AssociationEndMetaDataImpl(
                amd, targetName, source, targetCardinality, targetType,
                targetCascadeDelete, targetClearOnDelete, targetTargetCascadeDelete, targetTargetClearOnDelete);
            addAssociationEndMetaDataToEntityTypeSubtree(prepare(), target, targetEnd);
        }

        return amd;
    }

    private void addAssociationEndMetaDataToEntityTypeSubtree(Map<String, EntityMetaData> typeToEntityMetaDatas,
                                                              EntityMetaDataImpl emdi, AssociationEndMetaData aemd) {
        emdi.addAssociationEndMetaData(aemd);
        for (String subType : emdi.getAllSubTypes()) {
            ((EntityMetaDataImpl) typeToEntityMetaDatas.get(subType)).addAssociationEndMetaData(aemd);
        }
    }

    @Override
    public AssociationMetaData removeAssociation(String entityName, String associationName) {
        final Map<String, EntityMetaData> typeToEntityMetaDatas = prepare();

        // remove from source
        EntityMetaDataImpl source = (EntityMetaDataImpl) getEntityMetaData(entityName);
        AssociationEndMetaData aemd =
            removeAssociationEndMetaDataFromEntityTypeSubtree(typeToEntityMetaDatas, source, associationName);
        AssociationMetaData amd = aemd.getAssociationMetaData();


        // remove from target
        EntityMetaDataImpl target = (EntityMetaDataImpl) aemd.getOppositeEntityMetaData();
        if (amd.getType() != AssociationType.Directed) {
            String oppositeAssociationName = amd.getOppositeEnd(aemd).getName();
            removeAssociationEndMetaDataFromEntityTypeSubtree(typeToEntityMetaDatas, target, oppositeAssociationName);
        }

        associationMetaDatas.remove(getUniqueAssociationName(entityName, target.getType(),
            associationName));
        return amd;
    }

    private AssociationEndMetaData removeAssociationEndMetaDataFromEntityTypeSubtree(
        Map<String, EntityMetaData> typeToEntityMetaDatas, EntityMetaDataImpl emdi, String associationName) {
        AssociationEndMetaData removedEndMetaData = emdi.removeAssociationEndMetaData(associationName);
        for (String subType : emdi.getAllSubTypes()) {
            ((EntityMetaDataImpl) typeToEntityMetaDatas.get(subType)).removeAssociationEndMetaData(associationName);
        }
        return removedEndMetaData;
    }

    private static String getUniqueAssociationName(String sourceEntityName, String targetEntityName, String sourceName) {
        return sourceEntityName + '.' + sourceName + '-' + targetEntityName;
    }

}

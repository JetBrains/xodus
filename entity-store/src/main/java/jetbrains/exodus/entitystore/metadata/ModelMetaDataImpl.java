/**
 * Copyright 2010 - 2016 JetBrains s.r.o.
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
package jetbrains.exodus.entitystore.metadata;

import jetbrains.exodus.core.dataStructures.hash.HashMap;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;

public class ModelMetaDataImpl implements ModelMetaData {

    private Set<EntityMetaData> entityMetaDatas = new HashSet<>();
    private Map<String, AssociationMetaData> associationMetaDatas = new HashMap<>();
    private Map<String, EntityMetaData> typeToEntityMetaDatas = null;

    public void init() {
        reset();
        update();
    }

    public void setEntityMetaDatas(@NotNull Set<EntityMetaData> entityMetaDatas) {
        this.entityMetaDatas = entityMetaDatas;
        for (EntityMetaData emd : entityMetaDatas) {
            ((EntityMetaDataImpl) emd).setModelMetaData(this);
        }
        // init();
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
        typeToEntityMetaDatas = null;
    }

    void update() {
        if (typeToEntityMetaDatas != null) {
            return;
        }

        synchronized (this) {
            if (typeToEntityMetaDatas != null) {
                return;
            }
            typeToEntityMetaDatas = new HashMap<>();

            for (final EntityMetaData emd : entityMetaDatas) {
                ((EntityMetaDataImpl) emd).reset();

                final String type = emd.getType();
                if (typeToEntityMetaDatas.get(type) != null) {
                    throw new IllegalArgumentException("Duplicate entity [" + type + ']');
                }
                typeToEntityMetaDatas.put(type, emd);
            }

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
                    EntityMetaData parent = typeToEntityMetaDatas.get(superType);
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
                    addSubTypeToMetaData(emd, superType);
                }
                // add interface types
                for (String iFaceType : emd.getInterfaceTypes()) {
                    addSubTypeToMetaData(emd, iFaceType);
                }

                // set supertypes
                List<String> thisAndSuperTypes = new ArrayList<>();
                EntityMetaData data = emd;
                String t = data.getType();
                do {
                    thisAndSuperTypes.add(t);
                    thisAndSuperTypes.addAll(data.getInterfaceTypes());
                    data = typeToEntityMetaDatas.get(t);
                    t = data.getSuperType();
                } while (t != null);
                ((EntityMetaDataImpl) emd).setThisAndSuperTypes(thisAndSuperTypes);
            }
        }
    }

    private void addSubTypeToMetaData(EntityMetaData emd, String superType) {
        final EntityMetaData superEmd = typeToEntityMetaDatas.get(superType);
        if (superEmd == null) {
            throw new IllegalArgumentException("No entity metadata for super type [" + superType + "]");
        }
        ((EntityMetaDataImpl) superEmd).addSubType(emd.getType());
    }

    @Override
    @Nullable
    public EntityMetaData getEntityMetaData(@NotNull String entityType) {
        update();
        return typeToEntityMetaDatas.get(entityType);
    }

    @Override
    @NotNull
    public Iterable<EntityMetaData> getEntitiesMetaData() {
        update();
        return typeToEntityMetaDatas.values();
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
        addAssociationEndMetaDataToEntityTypeSubtree(source, sourceEnd);

        if (type != AssociationType.Directed) {
            AssociationEndMetaDataImpl targetEnd = new AssociationEndMetaDataImpl(
                    amd, targetName, source, targetCardinality, targetType,
                    targetCascadeDelete, targetClearOnDelete, targetTargetCascadeDelete, targetTargetClearOnDelete);
            addAssociationEndMetaDataToEntityTypeSubtree(target, targetEnd);
        }

        return amd;
    }

    private void addAssociationEndMetaDataToEntityTypeSubtree(EntityMetaDataImpl emdi, AssociationEndMetaData aemd) {
        emdi.addAssociationEndMetaData(aemd);
        for (String subType : emdi.getAllSubTypes()) {
            ((EntityMetaDataImpl) typeToEntityMetaDatas.get(subType)).addAssociationEndMetaData(aemd);
        }
    }

    @Override
    public AssociationMetaData removeAssociation(String entityName, String associationName) {

        // remove from source
        EntityMetaDataImpl source = (EntityMetaDataImpl) getEntityMetaData(entityName);
        AssociationEndMetaData aemd = removeAssociationEndMetaDataFromEntityTypeSubtree(source, associationName);
        AssociationMetaData amd = aemd.getAssociationMetaData();


        // remove from target
        EntityMetaDataImpl target = (EntityMetaDataImpl) aemd.getOppositeEntityMetaData();
        if (amd.getType() != AssociationType.Directed) {
            String oppositeAssociationName = amd.getOppositeEnd(aemd).getName();
            removeAssociationEndMetaDataFromEntityTypeSubtree(target, oppositeAssociationName);
        }

        associationMetaDatas.remove(getUniqueAssociationName(entityName, target.getType(),
                associationName));
        return amd;
    }

    private AssociationEndMetaData removeAssociationEndMetaDataFromEntityTypeSubtree(EntityMetaDataImpl emdi, String associationName) {
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

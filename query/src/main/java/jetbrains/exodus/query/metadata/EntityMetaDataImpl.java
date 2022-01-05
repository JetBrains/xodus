/**
 * Copyright 2010 - 2022 JetBrains s.r.o.
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
package jetbrains.exodus.query.metadata;

import jetbrains.exodus.core.dataStructures.NanoSet;
import jetbrains.exodus.core.dataStructures.decorators.HashMapDecorator;
import jetbrains.exodus.core.dataStructures.decorators.LinkedHashSetDecorator;
import jetbrains.exodus.core.dataStructures.hash.HashMap;
import jetbrains.exodus.core.dataStructures.hash.HashSet;
import jetbrains.exodus.entitystore.Entity;
import jetbrains.exodus.util.StringInterner;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class EntityMetaDataImpl implements EntityMetaData {

    private final AtomicReference<ModelMetaData> modelMetaData;
    private String type = null;
    private String superType = null;
    private final Set<String> interfaces = new LinkedHashSetDecorator<>();
    private Runnable initializer = null;
    private boolean removeOrphan = true;
    private boolean isAbstract = false;
    private final Set<String> subTypes = new LinkedHashSetDecorator<>();
    private List<String> thisAndSuperTypes = Collections.emptyList();
    private Set<AssociationEndMetaData> externalAssociationEnds = null;
    private final Map<String, PropertyMetaData> properties = new HashMapDecorator<>();
    private Set<Index> ownIndexes = Collections.emptySet();
    private Set<String> requiredProperties = Collections.emptySet();
    private Set<String> requiredIfProperties = Collections.emptySet();

    private volatile Map<String, Set<Index>> fieldToIndexes = null;
    private volatile Set<Index> indexes = null;
    private volatile List<String> allSubTypes = null;
    private volatile Map<String, Set<String>> incomingAssociations = null;
    private volatile Ends ends = null;

    public EntityMetaDataImpl() {
        this.modelMetaData = new AtomicReference<>();
    }

    public EntityMetaDataImpl(@NotNull ModelMetaData modelMetaData) {
        this.modelMetaData = new AtomicReference<>(modelMetaData);
    }

    void reset() {
        synchronized (this) {
            allSubTypes = null;
            incomingAssociations = null;
            indexes = null;
            fieldToIndexes = null;
            ends = null;
        }
    }

    void resetSelfAndSubtypes() {
        reset();

        for (String st : getSubTypes()) {
            ((EntityMetaDataImpl) getEntityMetaData(st)).reset();
        }
    }

    Set<AssociationEndMetaData> getExternalAssociationEnds() {
        return externalAssociationEnds;
    }

    @Override
    public ModelMetaData getModelMetaData() {
        return modelMetaData.get();
    }

    public void setModelMetaData(ModelMetaData modelMetaData) {
        if (!this.modelMetaData.compareAndSet(null, modelMetaData)) {
            throw new IllegalStateException("Cannot reuse EntityMetaDataImpl between " + modelMetaData + " and " + this.modelMetaData.get());
        }
    }

    public void setType(String type) {
        this.type = StringInterner.intern(type);
    }

    public void setSuperType(String superType) {
        this.superType = StringInterner.intern(superType);
        resetSelfAndSubtypes();
    }

    @Override
    public Iterable<String> getThisAndSuperTypes() {
        return thisAndSuperTypes;
    }

    // called by ModelMetadata.update after reset, so do not make reset itself
    void setThisAndSuperTypes(List<String> thisAndSuperTypes) {
        this.thisAndSuperTypes = thisAndSuperTypes;
    }

    @Override
    public boolean hasSubTypes() {
        return !subTypes.isEmpty();
    }

    @Override
    public boolean isAbstract() {
        return isAbstract;
    }

    public void setAbstract(boolean anAbstract) {
        isAbstract = anAbstract;
    }

    @Override
    public Collection<String> getSubTypes() {
        return subTypes;
    }

    @Override
    public Collection<String> getAllSubTypes() {
        if (!hasSubTypes()) return Collections.emptyList();
        List<String> result = allSubTypes;
        if (result == null) {
            synchronized (this) {
                result = allSubTypes;
                if (result == null) {
                    result = new ArrayList<>();
                    collectSubTypes(this, result);
                    allSubTypes = result;
                }
            }
        }
        return result;
    }

    private void collectSubTypes(EntityMetaDataImpl emd, List<String> result) {
        final Set<String> subTypes = emd.subTypes;
        result.addAll(subTypes);
        for (final String subType : subTypes) {
            collectSubTypes((EntityMetaDataImpl) modelMetaData.get().getEntityMetaData(subType), result);
        }
    }

    // called by ModelMetadata.update after reset, so do not make reset itself
    void addSubType(@NotNull String type) {
        subTypes.add(type);
    }

    public void setInitializer(Runnable initializer) {
        this.initializer = initializer;
    }

    @Override
    public Runnable getInitializer() {
        return initializer;
    }

    public void setRemoveOrphan(boolean removeOrphan) {
        this.removeOrphan = removeOrphan;
    }

    public void setIsAbstract(boolean anAbstract) {
        isAbstract = anAbstract;
    }

    public void setAssociationEndsMetaData(@NotNull Collection<AssociationEndMetaData> ends) {
        externalAssociationEnds = new HashSet<>();
        externalAssociationEnds.addAll(ends);
    }

    /*
     * For backward compatibility
     */
    public void setAssociationEnds(@NotNull Collection<AssociationEndMetaData> ends) {
        externalAssociationEnds = new HashSet<>();
        externalAssociationEnds.addAll(ends);
    }

    @Override
    public Collection<String> getInterfaceTypes() {
        return interfaces;
    }

    public void setInterfaces(List<String> interfaces) {
        this.interfaces.addAll(interfaces);
    }

    void addAssociationEndMetaData(AssociationEndMetaData end) {
        synchronized (this) {
            if (externalAssociationEnds == null) {
                externalAssociationEnds = new HashSet<>();
            }
            AssociationEndMetaData a = findAssociationEndMetaData(end.getName());
            if (a != null) {
                throw new IllegalArgumentException("Association already exists [" + end.getName() + ']');
            }
            externalAssociationEnds.add(end);
        }
        resetSelfAndSubtypes();
    }

    AssociationEndMetaData removeAssociationEndMetaData(String name) {
        try {
            synchronized (this) {
                AssociationEndMetaData a = findAssociationEndMetaData(name);
                if (a == null) {
                    throw new IllegalArgumentException("Can't find association end with name [" + name + ']');
                }
                externalAssociationEnds.remove(a);
                return a;
            }
        } finally {
            resetSelfAndSubtypes();
        }
    }

    private AssociationEndMetaData findAssociationEndMetaData(String name) {
        if (externalAssociationEnds != null) {
            for (AssociationEndMetaData a : externalAssociationEnds) {
                if (a.getName().equals(name)) {
                    return a;
                }
            }
        }
        return null;
    }

    @Override
    @NotNull
    public String getType() {
        return type;
    }

    @Override
    @Nullable
    public String getSuperType() {
        return superType;
    }

    @Override
    public AssociationEndMetaData getAssociationEndMetaData(@NotNull String name) {
        return getAssociationEnds().associationEnds.get(name);
    }

    @Override
    @NotNull
    public Collection<AssociationEndMetaData> getAssociationEndsMetaData() {
        return getAssociationEnds().associationEnds.values();
    }

    @Override
    public PropertyMetaData getPropertyMetaData(String name) {
        return properties.get(name);
    }

    @Override
    @NotNull
    public Iterable<PropertyMetaData> getPropertiesMetaData() {
        return properties.values();
    }

    public void setPropertiesMetaData(Iterable<PropertyMetaData> properties) {
        if (properties == null) return;
        for (PropertyMetaData p : properties) {
            this.properties.put(p.getName(), p);
        }
    }

    @Override
    public boolean getRemoveOrphan() {
        return removeOrphan;
    }

    @Override
    public boolean hasAggregationChildEnds() {
        return !getAssociationEnds().aggregationChildEnds.isEmpty();
    }

    @Override
    public Set<String> getAggregationChildEnds() {
        return getAssociationEnds().aggregationChildEnds;
    }

    @Override
    @NotNull
    public Map<String, Set<String>> getIncomingAssociations(final ModelMetaData mmd) {
        updateIncommingAssociations(mmd);
        return incomingAssociations;
    }

    private void updateIncommingAssociations(ModelMetaData mmd) {
        if (incomingAssociations == null) {
            synchronized (this) {
                if (incomingAssociations == null) {
                    final HashMapDecorator<String, Set<String>> result = new HashMapDecorator<>();
                    for (final EntityMetaData emd : mmd.getEntitiesMetaData()) {
                        for (final AssociationEndMetaData aemd : emd.getAssociationEndsMetaData()) {
                            if (type.equals(aemd.getOppositeEntityMetaData().getType())) {
                                collectLink(result, emd, aemd);
                            } else {
                                // if there are references to super type
                                Collection<String> associationEndSubtypes = aemd.getOppositeEntityMetaData().getAllSubTypes();
                                if (associationEndSubtypes.contains(type)) {
                                    collectLink(result, emd, aemd);
                                }
                            }
                        }
                    }
                    this.incomingAssociations = result;
                }
            }
        }
    }

    private void collectLink(Map<String, Set<String>> incomingAssociations, EntityMetaData emd, AssociationEndMetaData aemd) {
        final String associationName = aemd.getName();
        addIncomingAssociation(incomingAssociations, emd.getType(), associationName);
        //seems like we'll add them after in any case
//        for (final String subtype : emd.getSubTypes()) {
//            addIncomingAssociation(subtype, associationName);
//        }
    }

    private void addIncomingAssociation(@NotNull final Map<String, Set<String>> incomingAssociations,
                                        @NotNull final String type, @NotNull final String associationName) {
        Set<String> links = incomingAssociations.get(type);
        if (links == null) {
            links = new HashSet<>();
            incomingAssociations.put(type, links);
        }
        links.add(associationName);
    }

    @Override
    @NotNull
    public Set<Index> getOwnIndexes() {
        return ownIndexes;
    }

    @Override
    @NotNull
    public Set<Index> getIndexes() {
        Set<Index> result;
        while (true) {
            result = indexes;
            if (result != null) break;
            updateIndexes();
        }
        return result;
    }

    @Override
    @NotNull
    public Set<Index> getIndexes(String field) {
        Set<Index> result;
        while (true) {
            final Map<String, Set<Index>> fieldToIndexes = this.fieldToIndexes;
            if (fieldToIndexes != null) {
                result = fieldToIndexes.get(field);
                break;
            }
            updateIndexes();
        }
        return result == null ? Collections.emptySet() : result;
    }

    private void updateIndexes() {
        if (indexes == null || fieldToIndexes == null) {
            synchronized (this) {
                Set<Index> currentIndexes = indexes;
                if (currentIndexes == null) {
                    final Set<Index> result = new HashSet<>();
                    // add indexes of super types
                    for (String t : getThisAndSuperTypes()) {
                        for (Index index : getEntityMetaData(t).getOwnIndexes()) {
                            final String entityType = index.getOwnerEntityType();
                            for (String st : getEntityMetaData(entityType).getThisAndSuperTypes()) {
                                result.addAll(getEntityMetaData(st).getOwnIndexes());
                            }
                        }
                    }
                    currentIndexes = copySet(result);
                    indexes = currentIndexes;
                }
                if (fieldToIndexes == null) {
                    final HashMap<String, Set<Index>> result = new HashMap<>();
                    // build prop to ownIndexes map
                    for (Index index : currentIndexes) {
                        for (IndexField f : index.getFields()) {
                            Set<Index> fieldIndexes = result.get(f.getName());
                            if (fieldIndexes == null) {
                                fieldIndexes = new HashSet<>();
                                result.put(f.getName(), fieldIndexes);
                            }
                            fieldIndexes.add(index);
                        }
                    }
                    this.fieldToIndexes = result;
                }
            }
        }
    }

    private EntityMetaData getEntityMetaData(String type) {
        return modelMetaData.get().getEntityMetaData(type);
    }

    public void setOwnIndexes(Set<Index> ownIndexes) {
        this.ownIndexes = ownIndexes;
    }

    @Override
    @NotNull
    public Set<String> getRequiredProperties() {
        return requiredProperties;
    }

    @Override
    @NotNull
    public Set<String> getRequiredIfProperties(Entity e) {
        return requiredIfProperties;
    }

    public void setRequiredProperties(@NotNull Set<String> requiredProperties) {
        this.requiredProperties = copySet(requiredProperties);

    }

    public void setRequiredIfProperties(@NotNull Set<String> requiredIfProperties) {
        this.requiredIfProperties = copySet(requiredIfProperties);
    }

    @NotNull
    private Ends getAssociationEnds() {
        Ends result = ends;
        if (result == null) {
            synchronized (this) {
                result = ends;
                if (result == null) {
                    if (externalAssociationEnds == null) {
                        result = new Ends();
                    } else {
                        result = new Ends(
                            new HashMap<>(externalAssociationEnds.size()),
                            new LinkedHashSetDecorator<>());
                        for (final AssociationEndMetaData aemd : externalAssociationEnds) {
                            result.associationEnds.put(aemd.getName(), aemd);
                            if (aemd.getAssociationEndType() == AssociationEndType.ChildEnd) {
                                result.aggregationChildEnds.add(aemd.getName());
                            }
                        }
                    }
                    ends = result;
                }
            }
        }
        return result;
    }

    @Override
    public String toString() {
        return type;
    }

    private static <E> Set<E> copySet(@NotNull final Set<E> origin) {
        final int size = origin.size();
        if (size == 0) {
            return Collections.emptySet();
        }
        if (size == 1) {
            return new NanoSet<>(origin.iterator().next());
        }
        return new HashSet<>(origin);
    }

    private static class Ends {

        private final Map<String, AssociationEndMetaData> associationEnds;
        private final Set<String> aggregationChildEnds;

        private Ends() {
            this(Collections.emptyMap(), Collections.emptySet());
        }

        private Ends(@NotNull final Map<String, AssociationEndMetaData> associationEnds,
                     @NotNull final Set<String> aggregationChildEnds) {
            this.associationEnds = associationEnds;
            this.aggregationChildEnds = aggregationChildEnds;
        }
    }
}

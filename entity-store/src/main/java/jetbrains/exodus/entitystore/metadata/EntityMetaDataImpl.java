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

import jetbrains.exodus.core.dataStructures.decorators.HashMapDecorator;
import jetbrains.exodus.core.dataStructures.decorators.HashSetDecorator;
import jetbrains.exodus.core.dataStructures.hash.HashMap;
import jetbrains.exodus.core.dataStructures.hash.HashSet;
import jetbrains.exodus.entitystore.Entity;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;

public class EntityMetaDataImpl implements EntityMetaData {

    // original fields

    private ModelMetaData modelMetaData = null;
    private String type = null;
    private String superType = null;
    private Set<String> interfaces = new HashSetDecorator<>();
    private Runnable initializer = null;
    private boolean removeOrphan = true;
    private Set<String> subTypes = new HashSetDecorator<>();
    private List<String> thisAndSuperTypes = Collections.emptyList();
    private Set<AssociationEndMetaData> externalAssociationEnds = null;
    private Map<String, PropertyMetaData> properties = new HashMapDecorator<>();
    private Set<Index> ownIndexes = Collections.emptySet();
    private Set<String> requiredProperties = Collections.emptySet();
    private Set<String> requiredIfProperties = Collections.emptySet();
    private Set<String> historyIgnoredFields = Collections.emptySet();
    private Set<String> versionMismatchIgnored = Collections.emptySet();
    private boolean versionMismatchIgnoredForWholeClass = false;

    // calculated
    private Map<String, Set<Index>> fieldToIndexes = null;
    private Set<Index> indexes = null;
    private Set<String> aggregationChildEnds = null;
    private List<String> allSubTypes = null;
    private Map<String, Set<String>> incomingAssociations = null;
    private Map<String, AssociationEndMetaData> associationEnds = null;

    public EntityMetaDataImpl() {
    }

    public EntityMetaDataImpl(@NotNull ModelMetaData modelMetaData) {
        this.modelMetaData = modelMetaData;
    }

    void reset() {
        synchronized (this) {
            allSubTypes = null;
            associationEnds = null;
            incomingAssociations = null;
            indexes = null;
            fieldToIndexes = null;
            aggregationChildEnds = null;
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
        return modelMetaData;
    }

    public void setModelMetaData(ModelMetaData modelMetaData) {
        for (Index index : this.ownIndexes) {
            ((IndexImpl) index).setModelMetaData(modelMetaData);
        }

        this.modelMetaData = modelMetaData;
    }

    public void setType(String type) {
        if (type != null) {
            type = type.intern();
        }
        this.type = type;
    }

    public void setSuperType(String superType) {
        if (superType != null) {
            superType = superType.intern();
        }
        this.superType = superType;

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
    public Collection<String> getSubTypes() {
        return subTypes;
    }

    @Override
    public Collection<String> getAllSubTypes() {
        if (!hasSubTypes()) return Collections.emptyList();

        updateAllSubTypes();

        return allSubTypes;
    }

    private void updateAllSubTypes() {
        if (allSubTypes == null) {
            synchronized (this) {
                if (allSubTypes == null) {
                    List<String> _allSubTypes = new ArrayList<>();
                    collectSubTypes(this, _allSubTypes);
                    allSubTypes = _allSubTypes;
                }
            }
        }
    }

    private void collectSubTypes(EntityMetaDataImpl emd, List<String> result) {
        final Set<String> subTypes = emd.subTypes;
        result.addAll(subTypes);
        for (final String subType : subTypes) {
            collectSubTypes((EntityMetaDataImpl) modelMetaData.getEntityMetaData(subType), result);
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

    public void setHistoryIgnoredFields(Set<String> historyIgnoredFields) {
        this.historyIgnoredFields = historyIgnoredFields;
    }

    @Override
    public boolean isHistoryIgnored(String propertyName) {
        return historyIgnoredFields.contains(propertyName);
    }

    public void setRemoveOrphan(boolean removeOrphan) {
        this.removeOrphan = removeOrphan;
    }

    public void setAssociationEndsMetaData(@NotNull Collection<AssociationEndMetaData> ends) {
        externalAssociationEnds = new HashSet<>();
        externalAssociationEnds.addAll(ends);
    }

    /**
     * For backward compatibility
     *
     * @param ends
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

            resetSelfAndSubtypes();
            externalAssociationEnds.add(end);
        }
    }

    AssociationEndMetaData removeAssociationEndMetaData(String name) {
        synchronized (this) {
            AssociationEndMetaData a = findAssociationEndMetaData(name);

            if (a == null) {
                throw new IllegalArgumentException("Can't find association end with name [" + name + ']');
            }

            resetSelfAndSubtypes();
            externalAssociationEnds.remove(a);

            return a;
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
        updateAssociationEnds();
        return associationEnds.get(name);
    }

    @Override
    @NotNull
    public Collection<AssociationEndMetaData> getAssociationEndsMetaData() {
        updateAssociationEnds();
        return associationEnds.values();
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
        updateAssociationEnds();
        return !aggregationChildEnds.isEmpty();
    }

    @Override
    public Set<String> getAggregationChildEnds() {
        updateAssociationEnds();
        return aggregationChildEnds;
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
                    incomingAssociations = new HashMapDecorator<>();
                    for (final EntityMetaData emd : mmd.getEntitiesMetaData()) {
                        for (final AssociationEndMetaData aemd : emd.getAssociationEndsMetaData()) {
                            if (type.equals(aemd.getOppositeEntityMetaData().getType())) {
                                collectLink(emd, aemd);
                            } else {
                                // if there are references to super type
                                Collection<String> associationEndSubtypes = aemd.getOppositeEntityMetaData().getAllSubTypes();
                                if (associationEndSubtypes.contains(type)) {
                                    collectLink(emd, aemd);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private void collectLink(EntityMetaData emd, AssociationEndMetaData aemd) {
        final String associationName = aemd.getName();
        addIncomingAssociation(emd.getType(), associationName);
        //seems like we'll add them after in any case
//        for (final String subtype : emd.getSubTypes()) {
//            addIncomingAssociation(subtype, associationName);
//        }
    }

    private void addIncomingAssociation(@NotNull final String type, @NotNull final String associationName) {
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
        updateIndexes();

        return indexes;
    }

    @Override
    @NotNull
    public Set<Index> getIndexes(String field) {
        updateIndexes();

        Set<Index> res = fieldToIndexes.get(field);
        return res == null ? Collections.<Index>emptySet() : res;
    }

    private void updateIndexes() {
        if (indexes == null) {

            synchronized (this) {
                if (indexes == null) {
                    indexes = new HashSet<>();

                    // add indexes of super types
                    for (String t : getThisAndSuperTypes()) {
                        for (Index index : getEntityMetaData(t).getOwnIndexes()) {
                            final String enityType = index.getOwnerEntityType();
                            for (String st : getEntityMetaData(enityType).getThisAndSuperTypes()) {
                                indexes.addAll(getEntityMetaData(st).getOwnIndexes());
                            }
                        }
                    }
                }
            }
        }

        if (fieldToIndexes == null) {
            synchronized (this) {
                if (fieldToIndexes == null) {
                    fieldToIndexes = new HashMap<>();
                    // build prop to ownIndexes map
                    for (Index index : getIndexes()) {
                        for (IndexField f : index.getFields()) {
                            Set<Index> fieldIndexes = fieldToIndexes.get(f.getName());
                            if (fieldIndexes == null) {
                                fieldIndexes = new HashSet<>();
                                fieldToIndexes.put(f.getName(), fieldIndexes);
                            }
                            fieldIndexes.add(index);
                        }
                    }
                }
            }
        }
    }

    private EntityMetaData getEntityMetaData(String type) {
        return modelMetaData.getEntityMetaData(type);
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

    @Override
    public boolean isVersionMismatchIgnoredForWholeClass() {
        return versionMismatchIgnoredForWholeClass;
    }

    public void setVersionMismatchIgnoredForWholeClass(boolean versionMismatchIgnoredForWholeClass) {
        this.versionMismatchIgnoredForWholeClass = versionMismatchIgnoredForWholeClass;
    }

    public void setRequiredProperties(@NotNull Set<String> requiredProperties) {
        this.requiredProperties = requiredProperties;
    }

    public void setRequiredIfProperties(@NotNull Set<String> requiredIfProperties) {
        this.requiredIfProperties = requiredIfProperties;
    }

    @Override
    @Deprecated
    public boolean isVersionMismatchIgnored(@NotNull String propertyName) {
        return versionMismatchIgnored.contains(propertyName);
    }

    public void setVersionMismatchIgnored(@NotNull Set<String> versionMismatchIgnored) {
        this.versionMismatchIgnored = versionMismatchIgnored;
    }

    private void updateAssociationEnds() {
        if (associationEnds == null) {
            synchronized (this) {
                if (associationEnds == null) {
                    if (externalAssociationEnds == null) {
                        associationEnds = Collections.emptyMap();
                        aggregationChildEnds = Collections.emptySet();
                    } else {
                        associationEnds = new HashMap<>(externalAssociationEnds.size());
                        aggregationChildEnds = new HashSetDecorator<>();
                        for (final AssociationEndMetaData aemd : externalAssociationEnds) {
                            associationEnds.put(aemd.getName(), aemd);
                            if (aemd.getAssociationEndType() == AssociationEndType.ChildEnd) {
                                aggregationChildEnds.add(aemd.getName());
                            }
                        }
                    }
                }
            }
        }
    }

    @Deprecated
    public void setUniqueProperties(@NotNull Set<String> uniqueProperties) {
        //throw new UnsupportedOperationException("Regenerate your persistent models.");
    }

    @Override
    public String toString() {
        return type;
    }

}

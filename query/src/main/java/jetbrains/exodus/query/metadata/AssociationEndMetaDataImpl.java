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

import jetbrains.exodus.util.StringInterner;
import org.jetbrains.annotations.NotNull;

public class AssociationEndMetaDataImpl implements AssociationEndMetaData {

    private String name = null;
    private EntityMetaData emd = null;
    private String emdType = null;
    private AssociationEndCardinality cardinality = null;
    private String associationMetaDataName = null;
    private AssociationMetaData associationMetaData = null;
    private AssociationEndType type = null;
    private String oppositeEndName = null;
    private boolean cascadeDelete = false;
    private boolean clearOnDelete = false;
    private boolean targetCascadeDelete = false;
    private boolean targetClearOnDelete = false;

    public AssociationEndMetaDataImpl() {
    }

    public AssociationEndMetaDataImpl(AssociationMetaData associationMetaData, String name,
                                      EntityMetaData oppositeEndEntityType, AssociationEndCardinality cardinality,
                                      AssociationEndType type,
                                      boolean cascadeDelete, boolean clearOnDelete,
                                      boolean targetCascadeDelete, boolean targetClearOnDelete) {
        this.name = StringInterner.intern(name);
        emd = oppositeEndEntityType;
        emdType = oppositeEndEntityType.getType();
        this.cardinality = cardinality;
        setAssociationMetaDataInternal(associationMetaData);
        associationMetaDataName = ((AssociationMetaDataImpl) associationMetaData).getFullName();
        this.type = type;
        this.cascadeDelete = cascadeDelete;
        this.clearOnDelete = clearOnDelete;
        this.targetCascadeDelete = targetCascadeDelete;
        this.targetClearOnDelete = targetClearOnDelete;
    }

    void resolve(final ModelMetaDataImpl modelMetaData, final AssociationMetaData amd) {
        final EntityMetaData opposite = modelMetaData.getEntityMetaData(emdType);
        if (opposite == null) {
            throw new IllegalStateException("Can't find metadata for type: " + emdType + " from " + associationMetaDataName);
        }
        setOppositeEntityMetaDataInternal(opposite);
        setAssociationMetaDataInternal(amd);
    }

    @Override
    @NotNull
    public String getName() {
        return name;
    }

    @Override
    @NotNull
    public EntityMetaData getOppositeEntityMetaData() {
        return emd;
    }

    @NotNull
    String getOppositeEntityMetaDataType() {
        return emdType;
    }

    @NotNull
    String getAssociationMetaDataName() {
        return associationMetaDataName;
    }

    @Override
    @NotNull
    public AssociationEndCardinality getCardinality() {
        return cardinality;
    }

    @Override
    @NotNull
    public AssociationMetaData getAssociationMetaData() {
        return associationMetaData;
    }

    @Override
    @NotNull
    public AssociationEndType getAssociationEndType() {
        return type;
    }

    @Override
    public boolean getCascadeDelete() {
        return cascadeDelete;
    }

    @Override
    public boolean getClearOnDelete() {
        return clearOnDelete;
    }

    @Override
    public boolean getTargetCascadeDelete() {
        return targetCascadeDelete;
    }

    @Override
    public boolean getTargetClearOnDelete() {
        return targetClearOnDelete;
    }

    public void setName(@NotNull String name) {
        this.name = StringInterner.intern(name);
    }

    public void setOppositeEntityMetaDataType(@NotNull final String emdType) {
        this.emdType = emdType;
    }

    public void setCardinality(@NotNull AssociationEndCardinality cardinality) {
        this.cardinality = cardinality;
    }

    public void setAssociationMetaDataName(@NotNull String associationMetaDataName) {
        this.associationMetaDataName = associationMetaDataName;
    }

    public void setAssociationEndType(@NotNull AssociationEndType type) {
        this.type = type;
    }

    public void setCascadeDelete(boolean cascadeDelete) {
        this.cascadeDelete = cascadeDelete;
    }

    public void setClearOnDelete(boolean clearOnDelete) {
        this.clearOnDelete = clearOnDelete;
    }

    public void setTargetCascadeDelete(boolean cascadeDelete) {
        targetCascadeDelete = cascadeDelete;
    }

    public void setTargetClearOnDelete(boolean clearOnDelete) {
        targetClearOnDelete = clearOnDelete;
    }

    public String getOppositeEndName() {
        return oppositeEndName;
    }

    public void setOppositeEndName(String oppositeEndName) {
        this.oppositeEndName = oppositeEndName;
    }

    void setAssociationMetaDataInternal(@NotNull AssociationMetaData associationMetaData) {
        this.associationMetaData = associationMetaData;
        ((AssociationMetaDataImpl) this.associationMetaData).addEnd(this);
    }

    private void setOppositeEntityMetaDataInternal(@NotNull final EntityMetaData emd) {
        this.emd = emd;
    }
}

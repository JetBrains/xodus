package jetbrains.exodus.entitystore.orientdb

import jetbrains.exodus.entitystore.EntityId
import jetbrains.exodus.entitystore.EntityIterableHandle
import jetbrains.exodus.entitystore.EntityIterableType
import jetbrains.exodus.entitystore.iterate.EntityAddedOrDeletedHandleChecker
import jetbrains.exodus.entitystore.iterate.LinkChangedHandleChecker
import jetbrains.exodus.entitystore.iterate.PropertyChangedHandleChecker

class OEntityIterableHandle(private val query: String) : EntityIterableHandle {
    override fun hashCode(): Int {
        return 0
    }

    override fun equals(other: Any?): Boolean {
        return other is OEntityIterableHandle
    }

    override fun getType(): EntityIterableType {
        TODO("Not yet implemented")
    }

    override fun getIdentity(): Any? {
        TODO("Not yet implemented")
    }

    override fun isSticky(): Boolean {
        TODO("Not yet implemented")
    }

    override fun isMatchedEntityAdded(added: EntityId): Boolean {
        TODO("Not yet implemented")
    }

    override fun isMatchedEntityDeleted(deleted: EntityId): Boolean {
        TODO("Not yet implemented")
    }

    override fun isMatchedLinkAdded(
        source: EntityId,
        target: EntityId,
        linkId: Int
    ): Boolean {
        TODO("Not yet implemented")
    }

    override fun isMatchedLinkDeleted(
        source: EntityId,
        target: EntityId,
        linkId: Int
    ): Boolean {
        TODO("Not yet implemented")
    }

    override fun isMatchedPropertyChanged(
        id: EntityId,
        propertyId: Int,
        oldValue: Comparable<*>?,
        newValue: Comparable<*>?
    ): Boolean {
        TODO("Not yet implemented")
    }

    override fun onEntityAdded(handleChecker: EntityAddedOrDeletedHandleChecker): Boolean {
        TODO("Not yet implemented")
    }

    override fun onEntityDeleted(handleChecker: EntityAddedOrDeletedHandleChecker): Boolean {
        TODO("Not yet implemented")
    }

    override fun onLinkAdded(handleChecker: LinkChangedHandleChecker): Boolean {
        TODO("Not yet implemented")
    }

    override fun onLinkDeleted(handleChecker: LinkChangedHandleChecker): Boolean {
        TODO("Not yet implemented")
    }

    override fun onPropertyChanged(handleChecker: PropertyChangedHandleChecker): Boolean {
        TODO("Not yet implemented")
    }

    override fun getEntityTypeId(): Int {
        TODO("Not yet implemented")
    }

    override fun getLinkIds(): IntArray {
        TODO("Not yet implemented")
    }

    override fun getPropertyIds(): IntArray {
        TODO("Not yet implemented")
    }

    override fun getTypeIdsAffectingCreation(): IntArray {
        TODO("Not yet implemented")
    }

    override fun hasLinkId(id: Int): Boolean {
        TODO("Not yet implemented")
    }

    override fun isConsistent(): Boolean {
        TODO("Not yet implemented")
    }

    override fun resetBirthTime() {
        TODO("Not yet implemented")
    }

    override fun isExpired(): Boolean {
        TODO("Not yet implemented")
    }

    override fun toString(): String {
        return query
    }
}
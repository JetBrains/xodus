package jetbrains.exodus.entitystore

import jetbrains.exodus.entitystore.iterate.EntityIterableBase
import jetbrains.exodus.entitystore.iterate.SingleEntityIterable
import jetbrains.exodus.entitystore.iterate.SortIndirectIterable
import org.junit.Assert

class SortIndirectIterableTest : EntityStoreTestBase() {

    fun testIterateWithNullLinks() {
        val txn = storeTransaction

        // Create single Issue entity with no links.
        val issue = txn.newEntity(ENTITY_TYPE)
        txn.flush()

        // Iterate over all issues sorted by fixedInBuild link when sorted list of links is empty.
        checkSortedCount(txn, EntityIterableBase.EMPTY, 1)

        // The same, but the sorted list of links contains single null.
        val singleNullIterable = SingleEntityIterable(txn, null)
        checkSortedCount(txn, singleNullIterable, 1)

        // Set fixedInBuild link to our issue
        issue.setLink(LINK_NAME, txn.newEntity("Build"))
        txn.flush()

        // Sort one issue with link by the link.
        val singleNotNullIterable = SingleEntityIterable(txn, issue.getLink(LINK_NAME)!!.id)
        checkSortedCount(txn, singleNotNullIterable, 1)

        // Create another Issue entity with no links.
        txn.newEntity(ENTITY_TYPE)
        txn.flush()

        // Sort issues. Sorted links consists of single not null element.
        checkSortedCount(txn, singleNotNullIterable, 2)

        // Sort issues. Sorted links consists of null and not null elements.
        checkSortedCount(txn,
                singleNotNullIterable.union(singleNullIterable) as EntityIterableBase, 2)

        // Delete fixedInBuild link from the first issue
        issue.deleteLinks(LINK_NAME)
        txn.flush()

        // Sort issues with empty sortedLinks.
        checkSortedCount(txn, EntityIterableBase.EMPTY, 2)

        // Sort issues with sortedLinks containing single null.
        checkSortedCount(txn, singleNullIterable, 2)
    }

    private fun checkSortedCount(txn: PersistentStoreTransaction, sortedLinks: EntityIterableBase, count: Int) {
        var cnt = 0
        for (e in SortIndirectIterable(txn, entityStore, ENTITY_TYPE, sortedLinks, LINK_NAME,
                txn.getAll(ENTITY_TYPE) as EntityIterableBase, null, null)) {
            cnt++
        }
        Assert.assertEquals(count.toLong(), cnt.toLong())
    }

    companion object {

        private const val ENTITY_TYPE = "Issue"
        private const val LINK_NAME = "fixedInBuild"
    }
}

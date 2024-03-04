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
package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.record.ODirection
import com.orientechnologies.orient.core.record.OVertex
import java.io.File
import java.io.InputStream
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.entitystore.*

class OrientDBEntity(private val vertex: OVertex) : Entity {

    private var txid: Int = ODatabaseSession.getActiveSession().getTransaction().getId()

    override fun getStore(): EntityStore = throw UnsupportedOperationException()

    override fun getId(): EntityId = PersistentEntityId(vertex.identity.getClusterId(), vertex.identity.getClusterPosition())

    override fun toIdString(): String = vertex.identity.toString()

    override fun getType(): String = vertex.schemaClass!!.name

    override fun delete(): Boolean {
        vertex.delete()
        return false
    }

    override fun getRawProperty(propertyName: String): ByteIterable? = null

    override fun getProperty(propertyName: String): Comparable<*>? {
        reload()
        return vertex.getProperty<Comparable<*>>(propertyName)
    }

    private fun reload() {
        val session = ODatabaseSession.getActiveSession()
        val tx = session.transaction

        if (txid != tx.id) {
            txid = tx.id
            vertex.reload<OVertex>()
        }
    }

    override fun setProperty(propertyName: String, value: Comparable<*>): Boolean {
        reload()
        vertex.setProperty(propertyName, value)
        return false
    }

    override fun deleteProperty(propertyName: String): Boolean = false

    override fun getPropertyNames(): List<String> {
        reload()
        return ArrayList(vertex.getPropertyNames())
    }

    override fun getBlob(blobName: String): InputStream? = null

    override fun getBlobSize(blobName: String): Long = 0

    override fun getBlobString(blobName: String): String? = null

    override fun setBlob(blobName: String, blob: InputStream) {}

    override fun setBlob(blobName: String, file: File) {}

    override fun setBlobString(blobName: String, blobString: String): Boolean = false

    override fun deleteBlob(blobName: String): Boolean = false

    override fun getBlobNames(): List<String> = throw UnsupportedOperationException()

    override fun addLink(linkName: String, target: Entity): Boolean = false

    override fun addLink(linkName: String, targetId: EntityId): Boolean = false

    override fun getLink(linkName: String): Entity? = null

    override fun setLink(linkName: String, target: Entity?): Boolean = false

    override fun setLink(linkName: String, targetId: EntityId): Boolean = false

    override fun getLinks(linkName: String): EntityIterable {
        reload()
        return OrientDBLinksEntityIterable(vertex.getVertices(ODirection.OUT, linkName))
    }

    override fun getLinks(linkNames: Collection<String>): EntityIterable {
        reload()
        return OrientDBLinksEntityIterable(vertex.getVertices(ODirection.OUT, *linkNames.toTypedArray()))
    }

    override fun deleteLink(linkName: String, target: Entity): Boolean = false

    override fun deleteLink(linkName: String, targetId: EntityId): Boolean = false

    override fun deleteLinks(linkName: String) {}

    override fun getLinkNames(): List<String> {
        reload()
        return ArrayList(vertex.getEdgeNames(ODirection.OUT))
    }

    override fun compareTo(other: Entity): Int = 0
}
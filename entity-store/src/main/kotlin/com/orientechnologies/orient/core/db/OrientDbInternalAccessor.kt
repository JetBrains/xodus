package com.orientechnologies.orient.core.db

object OrientDbInternalAccessor {
    val OrientDB.accessInternal: OrientDBInternal
        get() {
            return this.getInternal()
        }
}

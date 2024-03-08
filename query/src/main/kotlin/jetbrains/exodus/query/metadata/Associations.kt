package jetbrains.exodus.query.metadata

data class LinkMetadata(
    val type1: String,
    val prop1: String,
    val cardinality1: AssociationEndCardinality,
    val type2: String,
    val prop2: String?,
    val cardinality2: AssociationEndCardinality?
) {
    val twoDirectional: Boolean = prop2 != null

    val name: String = if (prop2 == null)
        "${type1}_${prop1}_${type2}"
    else if (type1.lowercase() < type2.lowercase()) {
        "${type1}_${prop1}_${type2}_${prop2}"
    } else {
        "${type2}_${prop2}_${type1}_${prop1}"
    }

    override fun toString(): String = if (prop2 == null)
        "${type1}_${prop1}${cardinality1}_${type2}"
    else if (type1.lowercase() < type2.lowercase()) {
        "${type1}_${prop1}${cardinality1}_${type2}_${prop2}${cardinality2}"
    } else {
        "${type2}_${prop2}${cardinality2}_${type1}_${prop1}${cardinality1}"
    }
}
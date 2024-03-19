package ru.sozvezdie.filter.domain

import ru.sozvezdie.filter.common.BinaryToken
import ru.sozvezdie.filter.config.Constant

enum class TargetElementType(
    val typeName: String,
    val prefix: Byte,
    val hashFieldToId: (hashField: BinaryToken) -> Map<String, Any>
) {
    DISTRIBUTION(Distribution::class.java.simpleName, Constant.DISTRIBUTION_PREFIX, Distribution::hashFieldToId),
    REMNANT(Remnant::class.java.simpleName, Constant.REMNANT_PREFIX, Remnant::hashFieldToId);

    companion object {
        fun ofPrefix(prefix: Byte): TargetElementType = entries.firstOrNull { it.prefix == prefix }
            ?: throw RuntimeException("Unknown prefix for element: $prefix")
    }
}

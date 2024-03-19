package ru.sozvezdie.filter.domain

import ru.sozvezdie.filter.common.BinaryToken

interface TargetElement: Element {
    var clientId: Long
    var mapPharmacyId: String
    var batchId: String
    val hashName: BinaryToken
    val hashField: BinaryToken
    val checksum: Int
    fun joinBatch(batch: Batch)
}

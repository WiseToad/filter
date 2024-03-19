package ru.sozvezdie.filter.domain

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonPropertyOrder
import ru.sozvezdie.filter.common.ByteBuilder
import ru.sozvezdie.filter.common.*
import ru.sozvezdie.filter.config.Constant
import ru.sozvezdie.filter.parser.converter.MappedBindByName
import java.math.BigDecimal
import java.nio.ByteBuffer
import java.time.LocalDate
import java.util.zip.CRC32
import kotlin.properties.Delegates

@JsonPropertyOrder("clientId", "mapPharmacyId", "batch_id", "date")
open class Remnant: TargetElement {
    @set:MappedBindByName("client_id")
    override var clientId: Long by Delegates.notNull()

    @set:MappedBindByName("map_pharmacy_id")
    override lateinit var mapPharmacyId: String

    @set:MappedBindByName("batch_id")
    override lateinit var batchId: String // primary key component

    @set:MappedBindByName("date")
    lateinit var date: LocalDate

    @set:MappedBindByName("opening_balance")
    lateinit var openingBalance: BigDecimal

    @set:MappedBindByName("closing_balance")
    lateinit var closingBalance: BigDecimal

    @set:MappedBindByName("input_purchasing_price_balance")
    lateinit var inputPurchasingPriceBalance: BigDecimal

    @set:MappedBindByName("output_purchasing_price_balance")
    lateinit var outputPurchasingPriceBalance: BigDecimal

    @set:MappedBindByName("input_retail_price_balance")
    lateinit var inputRetailPriceBalance: BigDecimal

    @set:MappedBindByName("output_retail_price_balance")
    lateinit var outputRetailPriceBalance: BigDecimal

    // from batches
    var pharmEtalonId: Long? = null
    var mapNomenclatureCode: String? = null
    var mapNomenclatureName: String? = null
    var mapProducerCode: String? = null
    var mapProducerName: String? = null
    var mapProducerCountryCode: String? = null
    var mapProducerCountryName: String? = null
    var mapSupplierCode: String? = null
    var mapSupplierTin: String? = null
    var supplierName: String? = null
    var batchDocDate: LocalDate? = null
    var batchDocNumber: String? = null
    var purchasePriceNds: BigDecimal? = null
    var purchaseNds: Int? = null
    var retailPriceNds: BigDecimal? = null
    var retailNds: Int? = null
    var barcode: Long? = null
    var signCommission: Boolean? = null
    var internetZakaz: String? = null
    var nomenclatureCodes: NomenclatureCode? = null

    @get:JsonIgnore
    override val hashName: BinaryToken
        get() = ByteBuilder().add(Constant.REMNANT_PREFIX).add(clientId).add(mapPharmacyId, true).toBinaryToken()

    @get:JsonIgnore
    override val hashField: BinaryToken
        get() = ByteBuilder().add(batchId, true).add(date).toBinaryToken()

    @get:JsonIgnore
    override val checksum: Int
        get() = CRC32()
            .updateBy(openingBalance).updateBy(Constant.PART_DELIMITER)
            .updateBy(closingBalance).updateBy(Constant.PART_DELIMITER)
            .updateBy(inputPurchasingPriceBalance).updateBy(Constant.PART_DELIMITER)
            .updateBy(outputPurchasingPriceBalance).updateBy(Constant.PART_DELIMITER)
            .updateBy(inputRetailPriceBalance).updateBy(Constant.PART_DELIMITER)
            .updateBy(outputRetailPriceBalance).updateBy(Constant.PART_DELIMITER)
            .updateBy(pharmEtalonId).updateBy(Constant.PART_DELIMITER)
            .updateBy(mapNomenclatureName).updateBy(Constant.PART_DELIMITER)
            .updateBy(mapNomenclatureCode).updateBy(Constant.PART_DELIMITER)
            .updateBy(mapProducerCode).updateBy(Constant.PART_DELIMITER)
            .updateBy(mapSupplierCode).updateBy(Constant.PART_DELIMITER)
            .updateBy(mapSupplierTin).updateBy(Constant.PART_DELIMITER)
            .updateBy(batchDocDate).updateBy(Constant.PART_DELIMITER)
            .updateBy(batchDocNumber).updateBy(Constant.PART_DELIMITER)
            .updateBy(purchasePriceNds).updateBy(Constant.PART_DELIMITER)
            .updateBy(purchaseNds).updateBy(Constant.PART_DELIMITER)
            .updateBy(retailPriceNds).updateBy(Constant.PART_DELIMITER)
            .updateBy(retailNds).updateBy(Constant.PART_DELIMITER)
            .updateBy(barcode).updateBy(Constant.PART_DELIMITER)
            .updateBy(signCommission).updateBy(Constant.PART_DELIMITER)
            .updateBy(nomenclatureCodes).updateBy(Constant.PART_DELIMITER)
            .value.toInt()

    override fun joinBatch(batch: Batch) {
        barcode = batch.barcode
        pharmEtalonId = batch.pharmEtalonId
        mapNomenclatureCode = batch.mapNomenclatureCode
        mapNomenclatureName = batch.mapNomenclatureName
        mapProducerCode = batch.mapProducerCode
        mapProducerName = batch.mapProducerName
        mapProducerCountryCode = batch.mapProducerCountryCode
        mapProducerCountryName = batch.mapProducerCountryName
        mapSupplierCode = batch.mapSupplierCode
        mapSupplierTin = batch.mapSupplierTin
        supplierName = batch.supplierName
        batchDocDate = batch.batchDocDate
        batchDocNumber = batch.batchDocNumber
        purchasePriceNds = batch.purchasePriceNds
        purchaseNds = batch.purchaseNds
        retailPriceNds = batch.retailPriceNds
        retailNds = batch.retailNds
        barcode = batch.barcode
        signCommission = batch.signCommission
        internetZakaz = batch.internetZakaz
        nomenclatureCodes = batch.nomenclatureCodes
    }

    companion object {
        fun hashFieldToId(hashField: BinaryToken): Map<String, Any> {
            val bound = hashField.bytes.size - DATE_SIZE_BYTES
            return mapOf(
                "batchId" to ByteBuffer.wrap(hashField.bytes, 0, bound).getCompactedString(),
                "date" to ByteBuffer.wrap(hashField.bytes, bound, DATE_SIZE_BYTES).getLocalDate()
            )
        }
    }
}

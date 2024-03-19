package ru.sozvezdie.filter.domain

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonPropertyOrder
import ru.sozvezdie.filter.common.ByteBuilder
import ru.sozvezdie.filter.common.*
import ru.sozvezdie.filter.parser.converter.MappedBindByName
import ru.sozvezdie.filter.config.Constant
import java.math.BigDecimal
import java.nio.ByteBuffer
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.zip.CRC32
import kotlin.properties.Delegates

@JsonPropertyOrder("clientId", "mapPharmacyId", "distribution_id", "doc_date")
open class Distribution(): TargetElement {
    @set:MappedBindByName("client_id")
    override var clientId: Long by Delegates.notNull()

    @set:MappedBindByName("map_pharmacy_id")
    override lateinit var mapPharmacyId: String

    @set:MappedBindByName("distribution_id")
    lateinit var distributionId: String

    @get:JsonIgnore
    @set:MappedBindByName("batch_id")
    override lateinit var batchId: String

    @set:MappedBindByName("doc_date")
    lateinit var docDate: LocalDate

    @set:MappedBindByName("doc_type")
    var docType: Int? = null

    @set:MappedBindByName("doc_number")
    var docNumber: Int? = null

    @set:MappedBindByName("pos_number")
    var posNumber: String? = null

    @set:MappedBindByName("check_number")
    var checkNumber: Int? = null

    @set:MappedBindByName("check_unique_number")
    var checkUniqueNumber: String? = null

    @set:MappedBindByName("quantity")
    lateinit var quantity: BigDecimal

    @set:MappedBindByName("purchase_sum_nds")
    var purchaseSumNds: BigDecimal? = null

    @set:MappedBindByName("retail_sum_nds")
    var retailSumNds: BigDecimal? = null

    @set:MappedBindByName("discount_sum")
    var discountSum: BigDecimal? = null

    @set:MappedBindByName("cashier_id")
    var cashierId: String? = null

    @set:MappedBindByName("cashier_full_name")
    var cashierFullName: String? = null

    @set:MappedBindByName("cashier_tin")
    var cashierTin: String? = null

    @set:MappedBindByName("resale_sign")
    var resaleSign: Boolean? = null

    @set:MappedBindByName("fn_doc_number")
    var fnDocNumber: String? = null

    @set:MappedBindByName("fn_doc_sign")
    var fnDocSign: String? = null

    @set:MappedBindByName("fn_number")
    var fnNumber: String? = null

    @set:MappedBindByName("fn_doc_date")
    var fnDocDate: LocalDateTime? = null

    @set:MappedBindByName("internet_zakaz")
    var internetZakaz: String? = null

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
    var nomenclatureCodes: NomenclatureCode? = null

    constructor(remnant: Remnant): this() {
        clientId = remnant.clientId
        mapPharmacyId = remnant.mapPharmacyId
        distributionId = "${remnant.batchId}:${DateTimeFormatter.ISO_LOCAL_DATE.format(remnant.date)}"
        batchId = remnant.batchId
        docDate = remnant.date
        docType = Constant.REMNANT_AS_DISTRIBUTION_DOC_TYPE
        quantity = remnant.closingBalance
        purchaseSumNds = remnant.outputPurchasingPriceBalance
        retailSumNds = remnant.outputRetailPriceBalance
    }

    @get:JsonIgnore
    override val hashName: BinaryToken
        get() = ByteBuilder().add(Constant.DISTRIBUTION_PREFIX).add(clientId).add(mapPharmacyId, true).toBinaryToken()

    @get:JsonIgnore
    override val hashField: BinaryToken
        get() = ByteBuilder().add(distributionId, true).add(docDate).toBinaryToken()

    @get:JsonIgnore
    override val checksum: Int
        get() = CRC32()
            .updateBy(docType).updateBy(Constant.PART_DELIMITER)
            .updateBy(posNumber).updateBy(Constant.PART_DELIMITER)
            .updateBy(checkUniqueNumber).updateBy(Constant.PART_DELIMITER)
            .updateBy(quantity).updateBy(Constant.PART_DELIMITER)
            .updateBy(purchaseSumNds).updateBy(Constant.PART_DELIMITER)
            .updateBy(retailSumNds).updateBy(Constant.PART_DELIMITER)
            .updateBy(discountSum).updateBy(Constant.PART_DELIMITER)
            .updateBy(docNumber).updateBy(Constant.PART_DELIMITER)
            .updateBy(checkNumber).updateBy(Constant.PART_DELIMITER)
            .updateBy(docDate).updateBy(Constant.PART_DELIMITER)//todo doesn't exists in confluence
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
        nomenclatureCodes = batch.nomenclatureCodes
    }

    companion object {
        fun hashFieldToId(hashField: BinaryToken): Map<String, Any> {
            val bound = hashField.bytes.size - DATE_SIZE_BYTES
            return mapOf(
                "distributionId" to ByteBuffer.wrap(hashField.bytes, 0, bound).getCompactedString(),
                "docDate" to ByteBuffer.wrap(hashField.bytes, bound, DATE_SIZE_BYTES).getLocalDate()
            )
        }
    }
}

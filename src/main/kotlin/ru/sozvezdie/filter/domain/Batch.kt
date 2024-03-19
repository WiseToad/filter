package ru.sozvezdie.filter.domain

import com.fasterxml.jackson.annotation.JsonIgnore
import org.apache.commons.text.StringEscapeUtils
import ru.sozvezdie.filter.parser.converter.MappedBindByName
import java.math.BigDecimal
import java.time.LocalDate

class Batch : Element {
    @set:MappedBindByName(value = "map_batch_id")
    lateinit var mapBatchId: String

    @JsonIgnore
    @set:MappedBindByName("nomenclature_id")
    var pharmEtalonId: Long? = null

    @set:MappedBindByName("map_nomenclature_code")
    var mapNomenclatureCode: String? = null

    @set:MappedBindByName("map_nomenclature_name")
    var mapNomenclatureName: String? = null

    @set:MappedBindByName("map_producer_code")
    var mapProducerCode: String? = null

    @set:MappedBindByName("map_producer_name")
    var mapProducerName: String? = null
        set(value) {
            field = StringEscapeUtils.unescapeXml(value)
        }

    @set:MappedBindByName("map_producer_country_code")
    var mapProducerCountryCode: String? = null

    @set:MappedBindByName("map_producer_country_name")
    var mapProducerCountryName: String? = null
        set(value) {
            field = StringEscapeUtils.unescapeXml(value)
        }

    @set:MappedBindByName("map_supplier_code")
    var mapSupplierCode: String? = null

    @set:MappedBindByName("map_supplier_tin")
    var mapSupplierTin: String? = null

    @set:MappedBindByName("supplier_name")
    var supplierName: String? = null

    @set:MappedBindByName("batch_doc_date")
    var batchDocDate: LocalDate? = null

    @set:MappedBindByName("batch_doc_number")
    var batchDocNumber: String? = null

    @set:MappedBindByName("purchase_price_nds")
    var purchasePriceNds: BigDecimal? = null

    @set:MappedBindByName("purchase_nds")
    var purchaseNds: Int? = null

    @set:MappedBindByName("retail_price_nds")
    var retailPriceNds: BigDecimal? = null

    @set:MappedBindByName("retail_nds")
    var retailNds: Int? = null

    @set:MappedBindByName("barcode")
    var barcode: Long? = null

    var signCommission: Boolean? = null

    @set:MappedBindByName("internet_zakaz")
    var internetZakaz: String? = null

    var nomenclatureCodes: NomenclatureCode? = null

    val remnantSlim: RemnantSlim = RemnantSlim()

    class RemnantSlim {
        var dateMin: LocalDate? = null
        var dateMax: LocalDate? = null
        var quantityStart: BigDecimal = BigDecimal.ZERO
        var quantityEnd: BigDecimal = BigDecimal.ZERO
        var distributionQuantity: BigDecimal = BigDecimal.ZERO
    }

    @MappedBindByName("sign_comission", customMapping = true)
    fun convertAndSetSignCommission(value: String) {
        signCommission = value == "1"
    }

    @MappedBindByName("nomenclature_codes", customMapping = true)
    fun processAndSetNomenclatureCodes(nomenclatureCode: NomenclatureCode) {
        nomenclatureCode.pharmEtalon = pharmEtalonId
        nomenclatureCodes = nomenclatureCode
    }
}

package ru.sozvezdie.filter.domain

import ru.sozvezdie.filter.parser.converter.MappedBindByName
import java.time.LocalDate
import kotlin.properties.Delegates

class Action: Element {
    @set:MappedBindByName("type")
    lateinit var type: String

    @set:MappedBindByName("datestart")
    var startDate: LocalDate? = null

    @set:MappedBindByName("dateend")
    var endDate: LocalDate? = null

    var clientId: Long by Delegates.notNull()

    @set:MappedBindByName("map_pharmacy_ids")
    lateinit var mapPharmacyId: String
}

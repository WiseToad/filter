package ru.sozvezdie.filter.domain

import ru.sozvezdie.filter.parser.converter.MappedBindByName

class NomenclatureCode : Element {
    var pharmEtalon: Long? = null

    @set:MappedBindByName("puls")
    var puls: String? = null

    @set:MappedBindByName("sia")
    var sia: String? = null

    @set:MappedBindByName("protek")
    var protek: String? = null
}

package ru.sozvezdie.filter.domain

class NonArrivedElement(
    val typeName: String,
    val clientId: Long,
    val mapPharmacyId: String,
    val elementId: Map<String, Any>
): Element

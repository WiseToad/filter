package ru.sozvezdie.filter.domain

import com.fasterxml.jackson.annotation.JsonPropertyOrder
import java.time.LocalDate
import kotlin.properties.Delegates

@JsonPropertyOrder(
    "fileId", "fileNameOriginal", "fileSizeOriginal", "fileSizeParsed",
    "clientId", "mapPharmacyId", "startDate", "endDate",
    "elementCount", "uniqueCount", "nonArrivedCount"
)
class FileInfo(
    fileInfoDownloaded: FileInfoDownloaded
) {
    val fileId: Long = fileInfoDownloaded.fileId
    val fileNameOriginal: String = fileInfoDownloaded.fileNameOriginal
    val fileSizeOriginal: Long = fileInfoDownloaded.fileSizeOriginal
    var fileSizeParsed: Long = 0

    var clientId: Long by Delegates.notNull()
    lateinit var mapPharmacyId: String
    var startDate: LocalDate? = null
    var endDate: LocalDate? = null

    val elementCount = mutableMapOf<String, Int>()
    val uniqueCount = mutableMapOf<String, Int>()
    val nonArrivedCount = mutableMapOf<String, Int>()
}

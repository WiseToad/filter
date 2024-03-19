package ru.sozvezdie.filter.domain

class FileInfoDownloaded (
    val fileId: Long,
    val fileNameOriginal: String,
    val fileSizeOriginal: Long
) {
    val loggingTag get() = "fileId=$fileId, fileNameOriginal=$fileNameOriginal"
}

package ru.sozvezdie.filter.service

import org.springframework.stereotype.Service
import software.amazon.awssdk.core.ResponseInputStream
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.CopyObjectRequest
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.GetObjectResponse

@Service
class S3Service(
    private val s3Client: S3Client
) {
    fun getObject(bucket: String, key: String): ResponseInputStream<GetObjectResponse> =
        s3Client.getObject(
            GetObjectRequest
                .builder()
                .key(key)
                .bucket(bucket)
                .build()
        );

    fun renameObject(bucket: String, key: String, newKey: String) {
        s3Client.copyObject(
            CopyObjectRequest.builder()
                .sourceBucket(bucket)
                .sourceKey(key)
                .destinationBucket(bucket)
                .destinationKey(newKey)
                .build()
        )
        s3Client.deleteObject(
            DeleteObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build()
        )
    }

    fun getS3ObjectSize(response: ResponseInputStream<GetObjectResponse>): Long {
        return response.response().contentLength();
    }
}

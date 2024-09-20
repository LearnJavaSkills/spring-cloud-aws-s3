package in.learnjavaskills.springcloudawss3.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

/**
 * create bucket, list bucket, delete empty bucket, delete un-versioned bucket, delete version bucket and delete bucket website
 */
@Service
public class Bucket
{
    private final S3Client s3Client;

    @Autowired
    public Bucket(S3Client s3Client) {
        this.s3Client = s3Client;
    }

    /**
     * Create a empty bucket with the given name.
     * @param bucketName
     */
    public void createBucket(String bucketName) {
        try {
            CreateBucketRequest createBucketRequest = CreateBucketRequest.builder()
                    .bucket(bucketName)
                    .build();
            CreateBucketResponse createBucketResponse = s3Client.createBucket(createBucketRequest);
            boolean successful = createBucketResponse.sdkHttpResponse()
                    .isSuccessful();
            if (successful) {
                String location = createBucketResponse.location();
                System.out.println("Bucket created successfully at : " + location);
            } else System.out.println("Unable to create bucket");
        } catch (S3Exception s3Exception) {
            s3Exception.printStackTrace();
        }
    }

    /**
     * list down all the buckets
     */
    public void listBucketObjects() {
        try {
            ListBucketsResponse listBucketsResponse = s3Client.listBuckets();
            listBucketsResponse.buckets()
                    .stream()
                    .forEach(bucket -> System.out.println("bucket : " + bucket.name()));
        } catch (S3Exception s3Exception) {
            s3Exception.printStackTrace();
        }
    }

    /**
     * Delete empty Bucket
     * @param bucketName bucketName
     * @return true if bucket deleted else false
     */
    public boolean deleteEmptyBucket(String bucketName) {
        try {
            DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder()
                    .bucket(bucketName)
                    .build();
            DeleteBucketResponse deleteBucketResponse = s3Client.deleteBucket(deleteBucketRequest);
            boolean successful = deleteBucketResponse.sdkHttpResponse()
                    .isSuccessful();
            System.out.println("is bucket deleted : " + successful);
            return successful;
        } catch (S3Exception s3Exception) {
            s3Exception.printStackTrace();
            return false;
        }
    }

    /**
     * Delete  Unversioned bucket which has some object.
     * @param bucketName bucketName
     * @return isBucketDeleted
     */
    public boolean deleteUnversionedBucket(String bucketName) {
        try {
            // Before you can delete an Amazon S3 bucket, you must ensure that the bucket is empty or an error will result.
            // If you have a versioned bucket, you must also delete any versioned objects associated with the bucket.
            ListObjectsRequest listObjectsRequest = ListObjectsRequest.builder()
                    .bucket(bucketName)
                    .build();
            ListObjectsResponse listObjectsResponse = s3Client.listObjects(listObjectsRequest);
            listObjectsResponse.contents()
                    .stream()
                    // create a DeleteObjectRequest
                    .map(s3Object -> DeleteObjectRequest.builder()
                            .bucket(bucketName)
                            .key(s3Object.key())
                            .build())
                    // delete objects
                    .forEach(deleteObjectRequest -> s3Client.deleteObject(deleteObjectRequest));

            // once all the objects in a bucket deleted successfully, we can delete empty bucket.
            return deleteEmptyBucket(bucketName);
        } catch (S3Exception s3Exception) {
            s3Exception.printStackTrace();
            return false;
        }
    }

    /**
     * Delete version bucket
     * @param bucketName bucketName
     */
    public boolean deleteVersionedBucket(String bucketName) {
        try {
            // Before you can delete an Amazon S3 bucket, you must ensure that the bucket is empty or an error will result.
            // If you have a versioned bucket, you must also delete any versioned objects associated with the bucket.
            ListObjectVersionsRequest listObjectVersionsRequest = ListObjectVersionsRequest.builder()
                    .bucket(bucketName)
                    .build();
            ListObjectVersionsResponse listObjectVersionsResponse = s3Client.listObjectVersions(listObjectVersionsRequest);
            listObjectVersionsResponse.versions()
                    .stream()
                    // create a DeleteObjectRequest
                    .peek(objectVersion -> System.out.println("versionid: " + objectVersion.versionId() +
                            " key: " + objectVersion.key()))
                    .map(objectVersion -> DeleteObjectRequest.builder()
                            .bucket(bucketName)
                            .key(objectVersion.key())
                            .versionId(objectVersion.versionId())
                            .build())
                    // delete objets one by one
                    .forEach(deleteObjectRequest -> s3Client.deleteObject(deleteObjectRequest));

            // once all the objects in a bucket deleted successfully, we can delete empty bucket.
            return deleteEmptyBucket(bucketName);
        } catch (S3Exception s3Exception) {
            s3Exception.printStackTrace();
            return false;
        }
    }

    /**
     * delete bucket website, This action removes the website configurations for a bucket. Amazon S3 return a 200 OK
     * response upon successfully deleting website configuration on a specific bucket.
     *
     * You wil get 200 OK response if the website configuration you are trying to delete does not exist on the bucket.
     * Amazon S3 return a 404 response if bucket-specific in the request does not exist.
     * @param bucketName bucketName
     * @return true if bucket deleted else false
     */
    public boolean deleteBucketWebsite(String bucketName) {
        try {
            DeleteBucketWebsiteRequest deleteBucketWebsiteRequest = DeleteBucketWebsiteRequest.builder()
                    .bucket(bucketName)
                    .build();
            DeleteBucketWebsiteResponse deleteBucketWebsiteResponse = s3Client.deleteBucketWebsite(deleteBucketWebsiteRequest);
            boolean successful = deleteBucketWebsiteResponse.sdkHttpResponse()
                    .isSuccessful();
            System.out.println("Is S3 Website Bucket Deleted : " + successful);
            return successful;
        } catch (S3Exception s3Exception) {
            s3Exception.printStackTrace();
            return false;
        }
    }

}

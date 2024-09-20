package in.learnjavaskills.springcloudawss3.service;

import io.awspring.cloud.s3.S3Exception;
import io.awspring.cloud.s3.S3Template;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedPutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;

import java.net.URL;
import java.time.Duration;

/**
 * Create the presigned URL for GET and POST request using the S3Template and S3Presigner.
 */
@Service
public class PreSignedUrlObject
{
    private final S3Template s3Template;
    private final S3Presigner s3Presigner;

    @Autowired
    public PreSignedUrlObject(S3Template s3Template, S3Presigner s3Presigner) {
        this.s3Template=s3Template;
        this.s3Presigner = s3Presigner;
    }

    /**
     * Create a preSigned URL of s3 object using S3Template to download an object in a subsequent GET request.
     * @param bucketName bucketName
     * @param key object key
     * @return presigned URL
     */
    public URL createPreSignedUrlUsingS3TemplateForGetRequest(String bucketName, String key) {
        try {
            return s3Template.createSignedGetURL(bucketName, key, Duration.ofMinutes(10));
        } catch (S3Exception s3Exception) {
            s3Exception.printStackTrace();
            return null;
        }
    }

    /**
     * Create a preSigned URL of s3 object using S3Template to upload an object in a subsequent PUT request.
     * @param bucketName bucketName
     * @param key object key
     * @return presigned URL
     */
    public URL createPreSignedUrlUsingS3TemplateForPutRequest(String bucketName, String key) {
        try {
            return s3Template.createSignedPutURL(bucketName, key, Duration.ofMinutes(10));
        } catch (S3Exception s3Exception) {
            s3Exception.printStackTrace();
            return null;
        }
    }

    /**
     * Create a pre-signed URL using S3Presigner to download an object in a subsequent GET request.
     * @param bucketName bucketName
     * @param key object key
     * @return presigned URL
     */
    public URL createPreSignedUrlUsingS3PresignerForGetRequest(String bucketName, String key) {
        try
        {
            GetObjectRequest objectRequest = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();

            GetObjectPresignRequest objectPresignRequest = GetObjectPresignRequest.builder()
                    .signatureDuration(Duration.ofMinutes(10)) // URL will expire after 10 minutes
                    .getObjectRequest(objectRequest)
                    .build();
            PresignedGetObjectRequest presignedGetObjectRequest = s3Presigner.presignGetObject(objectPresignRequest);

            URL url = presignedGetObjectRequest.url();
            System.out.println("presigned url : " + url.toString());
            return url;
        } catch (S3Exception s3Exception) {
            s3Exception.printStackTrace();
            return null;
        }
    }

    /**
     * Create a pre-signed URL using S3Presigner to upload an object in a subsequent PUT request.
     * @param bucketName bucketName
     * @param key object key
     * @return presigned URL
     */
    public URL createPreSignedUrlUsingS3PresignerForPutRequest(String bucketName, String key) {
        try {
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();
            PutObjectPresignRequest putObjectPresignRequest = PutObjectPresignRequest.builder()
                    .signatureDuration(Duration.ofMinutes(10)) // URL will expire after 10 minutes
                    .putObjectRequest(putObjectRequest)
                    .build();

            PresignedPutObjectRequest presignedPutObjectRequest = s3Presigner.presignPutObject(putObjectPresignRequest);
            URL url = presignedPutObjectRequest.url();
            System.out.println("presigned url : " + url.toString());
            return  url;
        } catch (S3Exception s3Exception) {
            s3Exception.printStackTrace();
            return null;
        }
    }
}

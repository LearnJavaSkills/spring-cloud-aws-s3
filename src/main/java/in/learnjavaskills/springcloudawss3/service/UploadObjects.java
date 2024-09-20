package in.learnjavaskills.springcloudawss3.service;

import io.awspring.cloud.s3.ObjectMetadata;
import io.awspring.cloud.s3.S3Resource;
import io.awspring.cloud.s3.S3Template;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedFileUpload;
import software.amazon.awssdk.transfer.s3.model.FileUpload;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

/**
 * Upload file in S3 using various API such as s3Client, s3TransferManager and s3Template
 */
@Service
public class UploadObjects
{
    private final S3Client s3Client;
    private final S3TransferManager s3transferManager;

    private final S3Template s3Template;

    @Autowired
    public UploadObjects(S3Client s3Client, S3TransferManager s3TransferManager, S3Template s3Template) {
        this.s3Client = s3Client;
        this.s3transferManager = s3TransferManager;
        this.s3Template = s3Template;
    }

    /**
     * Upload file using s3Client
     * @param bucketName name of the bucket
     * @param key desire key - usually the name of the file
     * @param filePath absolute file path to upload in s3
     */
    public void uploadFileUsingS3Client(String bucketName, String key, String filePath) {
        try {
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();
            s3Client.putObject(putObjectRequest, RequestBody.fromFile(Paths.get(filePath)));
        } catch (S3Exception s3Exception) {
            s3Exception.printStackTrace();
        }
    }

    /**
     * Upload a file with tag using s3 client
     * @param bucketName name of the bucket
     * @param key desire key - usually the name of the file
     * @param filePath absolute file path to upload in s3
     */
    public void uploadFileWithTag(String bucketName, String key, String filePath) {
        try {
            Tag tag1 = Tag.builder().
                    key("tag 1")
                    .value("tag 1 value")
                    .build();
            Tag tag2 = Tag.builder()
                    .key("tag 2")
                    .value("tag 2 value")
                    .build();

            List<Tag> tagList = List.of(tag1, tag2);
            Tagging tagging = Tagging.builder()
                    .tagSet(tagList)
                    .build();

            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .tagging(tagging)
                    .build();
            s3Client.putObject(putObjectRequest, RequestBody.fromFile(Paths.get(filePath)));
        } catch (S3Exception s3Exception) {
            s3Exception.printStackTrace();
        }
    }

    /**
     * update tags in s3 objects
     * @param bucketName
     * @param key
     */
    public void updateObjectTag(String bucketName, String key) {
        try {
            GetObjectTaggingRequest getObjectTaggingRequest = GetObjectTaggingRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();
            GetObjectTaggingResponse objectTagging = s3Client.getObjectTagging(getObjectTaggingRequest);

            // print existing tags
            objectTagging.tagSet()
                    .stream()
                    .forEach(tag -> System.out.println("tag key : " + tag.key() + " tag value : " + tag.value()));

            // new tags add
            Tag tag3 = Tag.builder()
                    .key("tag 3")
                    .value("tag 3 value")
                    .build();
            Tag tag4 = Tag.builder()
                    .key("tag 4")
                    .value("tag 4 value")
                    .build();

            List<Tag> tagList = List.of(tag3, tag4);
            Tagging updatedTags = Tagging.builder()
                    .tagSet(tagList)
                    .build();

            PutObjectTaggingRequest putObjectTaggingRequest = PutObjectTaggingRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .tagging(updatedTags)
                    .build();

            // update tags
            s3Client.putObjectTagging(putObjectTaggingRequest);
        } catch (S3Exception s3Exception) {
            s3Exception.printStackTrace();
        }
    }

    /**
     * upload file with metadata using s3 client
     * @param bucketName name of the bucket
     * @param key desire key - usually the name of the file
     * @param filePath absolute file path to upload in s3
     */
    public void uploadFileWithMetaData(String bucketName, String key, String filePath) {
        try {
            Map<String, String> metadata = new HashMap<>();
            metadata.put("author", "learnjavaskills.in");
            metadata.put("file-type", "txt");
            metadata.put("version", "1.0");

            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .metadata(metadata)
                    .build();
            s3Client.putObject(putObjectRequest, RequestBody.fromFile(Paths.get(filePath)));
        } catch (S3Exception s3Exception) {
            s3Exception.printStackTrace();
        }
    }

    /**
     * Upload file using s3transferManager
     * @param bucketName name of the bucket
     * @param key desire key - usually the name of the file
     * @param filePath absolute file path to upload in s3
     */
    public void uploadFileWithTransferManager(String bucketName, String key, String filePath) {
        try
        {
            UploadFileRequest uploadFileRequest = UploadFileRequest.builder()
                    .putObjectRequest(builder -> builder.bucket(bucketName)
                            .key(key))
                    .source(Paths.get(filePath))
                    .build();
            FileUpload fileUpload = s3transferManager.uploadFile(uploadFileRequest);
            // check progress of file upload
            while (true)
            {
                OptionalLong optionalLong = fileUpload.progress()
                        .snapshot()
                        .remainingBytes();
                if (optionalLong.isPresent())
                {
                    System.out.println("remaining byte transfer : " + optionalLong.getAsLong());
                    if (optionalLong.getAsLong() == 0)
                        break;
                }
            }

            CompletedFileUpload completedFileUpload = fileUpload.completionFuture().join();
            boolean successful = completedFileUpload.response().sdkHttpResponse().isSuccessful();
            System.out.println("is Successful : " + successful);
        } catch (S3Exception s3Exception) {
            s3Exception.printStackTrace();
        }
    }

    /**
     * upload file using s3Template
     * @param bucketName name of the bucket
     * @param key desire key - usually the name of the file
     * @param filePath absolute file path to upload in s3
     */
    public void uploadFileUsingS3Template(String bucketName, String key, String filePath) {
        try {
            InputStream inputStream = new FileInputStream(filePath);
            S3Resource s3Resource = s3Template.upload(bucketName, key, inputStream);
            URL url = s3Resource.getURL();
            System.out.println("File uploaded successfully at " + url.toString());
        } catch (S3Exception | IOException exception) {
            exception.printStackTrace();
        }
    }

    /**
     * upload file using s3Template with objectMetaData
     * @param bucketName name of the bucket
     * @param key desire key - usually the name of the file
     * @param filePath absolute file path to upload in s3
     */
    public void uploadFileUsingS3TemplateWithMetadata(String bucketName, String key, String filePath) {
        try {
            InputStream inputStream = new FileInputStream(filePath);
            ObjectMetadata objectMetadata = ObjectMetadata.builder()
                    .metadata("author", "learnjavaskills.in")
                    .metadata("version", "1.0")
                    .contentType("text/plain")
                    .build();
            S3Resource s3Resource = s3Template.upload(bucketName, key, inputStream, objectMetadata);
            URL url = s3Resource.getURL();
            System.out.println("File uploaded successfully at " + url.toString());
        }catch (S3Exception | IOException exception) {
            exception.printStackTrace();
        }
    }
}

package in.learnjavaskills.springcloudawss3.service;

import io.awspring.cloud.s3.S3Resource;
import io.awspring.cloud.s3.S3Template;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.*;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * Download file using STemplate, S3Client and S3TransferManager
 */
@Service
public class DownloadObjects
{
    private final S3Template s3Template;
    private final S3Client s3Client;

    private final S3TransferManager s3TransferManager;

    @Autowired
    public DownloadObjects(S3Template s3Template, S3Client s3Client, S3TransferManager s3TransferManager) {
        this.s3Template = s3Template;
        this.s3Client = s3Client;
        this.s3TransferManager = s3TransferManager;
    }

    /**
     * download file using S3Client
     * @param bucketName name of the bucket
     * @param key desire key - usually the name of the file
     * @param downloadDestinationPath absolute file path to download from s3
     */
    public void downloadFileUsingS3Client(String bucketName, String key, String downloadDestinationPath) {
        try {
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();
            s3Client.getObject(getObjectRequest, Path.of(downloadDestinationPath));
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }

    /**
     * download file using s3TransferManager
     * @param bucketName name of the bucket
     * @param key desire key - usually the name of the file
     * @param downloadDestinationPath absolute file path to download from s3
     */
    public void downloadFileUsingS3TransferManager(String bucketName, String key, String downloadDestinationPath) {
        try {
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();

            DownloadFileRequest downloadFileRequest = DownloadFileRequest.builder()
                    .destination(Path.of(downloadDestinationPath))
                    .getObjectRequest(getObjectRequest)
                    .build();
            FileDownload fileDownload = s3TransferManager.downloadFile(downloadFileRequest);
            CompletedFileDownload completedFileDownload = fileDownload.completionFuture()
                    .join();
            String contentType = completedFileDownload.response()
                    .contentType();
            System.out.println("Download completed, content type is : " + contentType);
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }

    /**
     * download bucket with all the objects in side bucket using S3TransferManager.
     * @param bucketName name of the bucket
     * @param downloadDestinationPath absolute directory path to download from s3
     */
    public void downloadDirectoryUsingS3TransferManager(String bucketName, String downloadDestinationPath) {
        try {
            DownloadDirectoryRequest downloadDirectoryRequest = DownloadDirectoryRequest.builder()
                    .bucket(bucketName)
                    .destination(Path.of(downloadDestinationPath))
                    .build();
            DirectoryDownload directoryDownload = s3TransferManager.downloadDirectory(downloadDirectoryRequest);
            CompletedDirectoryDownload completedDirectoryDownload = directoryDownload.completionFuture()
                    .join();
            completedDirectoryDownload.failedTransfers()
                    .forEach(failedFileDownload -> System.out.println("fail to download : " + failedFileDownload.toString()));
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }

    /**
     * download files using S3Template
     * @param bucketName name of the bucket
     * @param key desire key - usually the name of the file
     * @param downloadDestinationPath absolute file path to download from s3
     */
    public void downloadFileUsingS3Template(String bucketName, String key, String downloadDestinationPath) {
        try {
            S3Resource s3Resource = s3Template.download(bucketName, key);
            InputStream inputStream = s3Resource.getInputStream();
            Files.copy(inputStream, Path.of(downloadDestinationPath), StandardCopyOption.REPLACE_EXISTING);
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }
}

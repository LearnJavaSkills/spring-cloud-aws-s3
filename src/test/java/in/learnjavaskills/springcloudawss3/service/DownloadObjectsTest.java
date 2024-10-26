package in.learnjavaskills.springcloudawss3.service;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class DownloadObjectsTest
{
    @Autowired private DownloadObjects downloadObjects;

    private final String bucketName = "learnjavaskills";

    private final String key = "hat.png";

    private final String destinationPath = "src/test/resources/hat.png";

    private final String destinationPathForDirectory = "src/test/resources";

    private final String csvFileName = "sample-file.csv";

    @Test
    void downloadFileUsingS3Client()
    {
        downloadObjects.downloadFileUsingS3Client(bucketName, key, destinationPath);
    }

    @Test
    void readFileWithoutDownloadingUsingS3Client() {
        downloadObjects.readFileUsingS3Client(bucketName, csvFileName);
    }

    @Test
    void downloadFileUsingS3TransferManager()
    {
        downloadObjects.downloadFileUsingS3TransferManager(bucketName, key, destinationPath);
    }

    @Test
    void downloadDirectoryUsingS3TransferManager()
    {
        downloadObjects.downloadDirectoryUsingS3TransferManager(bucketName, destinationPathForDirectory);
    }

    @Test
    void downloadFileUsingS3Template()
    {
        downloadObjects.downloadFileUsingS3Template(bucketName, key, destinationPath);
    }
}
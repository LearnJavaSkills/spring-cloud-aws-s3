package in.learnjavaskills.springcloudawss3.service;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class UploadObjectsTest
{

    @Autowired
    private UploadObjects uploadObjects;

    private final String bucketName = "learnjavaskills";
    private final String filePath = "";


    @Test
    void uploadFileUsingS3Client()
    {
        uploadObjects.uploadFileUsingS3Client(bucketName, "robot.gif", filePath);
    }

    @Test
    void uploadFileWithTag()
    {
        uploadObjects.uploadFileWithTag(bucketName, "robot-with-tag.gif", filePath);
    }

    @Test
    void updateObjectTag()
    {
        uploadObjects.updateObjectTag(bucketName, "robot-with-tag.gif");
    }

    @Test
    void uploadFileWithMetaData()
    {
        uploadObjects.uploadFileWithMetaData(bucketName, "robot-with-meta-data.gif", filePath);
    }

    @Test
    void uploadFileWithTransferManager()
    {
        uploadObjects.uploadFileWithTransferManager(bucketName, "robot-with-file-transfer-upload.gif", filePath);
    }

    @Test
    void uploadFileUsingS3Template()
    {
        uploadObjects.uploadFileUsingS3Template(bucketName, "robot-using-s3-template.gif", filePath);
    }

    @Test
    void uploadFileUsingS3TemplateWithMetadata()
    {
        uploadObjects.uploadFileUsingS3TemplateWithMetadata(bucketName, "robot-s3-template-metadata.gif", filePath);
    }
}
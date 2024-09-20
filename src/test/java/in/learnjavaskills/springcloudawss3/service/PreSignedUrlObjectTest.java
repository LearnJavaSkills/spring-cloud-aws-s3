package in.learnjavaskills.springcloudawss3.service;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.net.URL;

@SpringBootTest
class PreSignedUrlObjectTest
{

    @Autowired
    private PreSignedUrlObject preSignedUrlObject;

    private final String bucketName = "learnjavaskills";

    private final String key = "hat.png";

    private final String key2 = "index.html";

    @Test
    void createPreSignedUrlUsingS3TemplateForGetRequest()
    {
        URL url = preSignedUrlObject.createPreSignedUrlUsingS3TemplateForGetRequest(bucketName, key);
        System.out.println("URL : " + url.toString());
    }

    @Test
    void createPreSignedUrlUsingS3TemplateForPostRequest()
    {
        URL url = preSignedUrlObject.createPreSignedUrlUsingS3TemplateForPutRequest(bucketName, key2);
        System.out.println("post url : " + url.toString());
    }

    @Test
    void createPreSignedUrlUsingS3PresignerForGetRequest()
    {
        URL url = preSignedUrlObject.createPreSignedUrlUsingS3PresignerForGetRequest(bucketName, key2);
        System.out.println("URL using s3 presigner : " + url);
    }

    @Test
    void createPreSignedUrlUsingS3PresignerForPostRequest()
    {
        URL url = preSignedUrlObject.createPreSignedUrlUsingS3PresignerForPutRequest(bucketName, key2);
        System.out.println("s3 presigner url post : " + url.toString());
    }
}
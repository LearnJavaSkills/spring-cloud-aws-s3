package in.learnjavaskills.springcloudawss3.service;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.stereotype.Service;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class BucketTest
{
    @Autowired private Bucket bucket;

    public static final String LEARNJAVASKILLS = "website-learnjavaskills";

    @Test
    void createBucket()
    {
        bucket.createBucket(LEARNJAVASKILLS);
    }

    @Test
    void listBucketObjects()
    {
        bucket.listBucketObjects();
    }

    @Test
    void deleteEmptyBucket()
    {
        boolean isDeleted = bucket.deleteEmptyBucket(LEARNJAVASKILLS);
        assertTrue(isDeleted);
    }

    @Test
    void deleteUnversionedBucket()
    {
        boolean isDeleted = bucket.deleteUnversionedBucket(LEARNJAVASKILLS);
        assertTrue(isDeleted);
    }

    @Test
    void deleteVersionedBucket()
    {
        boolean isDeleted = bucket.deleteVersionedBucket(LEARNJAVASKILLS);
        assertTrue(isDeleted);
    }

    @Test
    void deleteBucketWebsite()
    {
        boolean isDeleted = bucket.deleteBucketWebsite(LEARNJAVASKILLS);
        assertTrue(isDeleted);
    }
}
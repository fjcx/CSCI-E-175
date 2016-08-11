package com.fjcx.e175;

import java.io.File;
import java.io.IOException;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * E-175 Cloud Computing
 * Assignment 5
 * Problem 1
 * Student: Frank O'Connor
 * Date: 10/08/12
 *
 * Class lists all files in a directory "readFolder" and uploads them to S3 bucket "fjcx05prob01".
 */
public class Prob1 {

    public static void main(String[] args) throws IOException {
    	// Create Amazon S3 Client
    	AmazonS3 s3 = new AmazonS3Client(new PropertiesCredentials(Prob1.class.getResourceAsStream("/AwsCredentials.properties")));

        String bucketName = "fjcx05prob01";
        String key = "";
        String dirPath = "readFolder"; 
        String filePath = "";

        try {     
            // Create a new S3 bucket     
            System.out.println("Creating bucket " + bucketName + "\n");
            s3.createBucket(bucketName);

            // List the buckets in account
            System.out.println("Listing buckets");
            for (Bucket bucket : s3.listBuckets()) {
                System.out.println(" - " + bucket.getName());
            }

            // List files in read directory        
            File folder = new File(dirPath);
            File[] listOfFiles = folder.listFiles(); 
           
            // Upload each file in the directory to s3 Bucket
            for (int i = 0; i < listOfFiles.length; i++) {
				if (listOfFiles[i].isFile()){
					filePath = dirPath+"/"+listOfFiles[i].getName();
					File s3UploadFile = new File(filePath);
					System.out.println("Uploading a new object to S3 from a file: "+filePath);
					// uploading with s3 key as same dirName structure
					key = filePath;
		            s3.putObject(new PutObjectRequest(bucketName, key, s3UploadFile));
				}
            }

            // List objects in bucket
            System.out.println("\nListing objects");
            ObjectListing objectListing = s3.listObjects(new ListObjectsRequest().withBucketName(bucketName));
            for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                System.out.println(" - " + objectSummary.getKey() + "  " + "(size = " + objectSummary.getSize() + ")");
            }
      
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    }
}

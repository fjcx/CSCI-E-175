package com.fjcx.e175;

import java.io.IOException;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * E-175 Cloud Computing
 * Assignment 5
 * Problem 2
 * Student: Frank O'Connor
 * Date: 10/08/12
 * 
 * Class accepts an argument from the command line. It then searches for an existing S3 bucket with a name that matches the arg.
 * If a matching bucket exists, then the class deletes all objects within the bucket and then the bucket itself.
 * Progam usage: java Classname bucketName
 */
public class Prob2 {

    public static void main(String[] args) throws IOException {
    	// Create Amazon S3 Client
    	AmazonS3 s3 = new AmazonS3Client(new PropertiesCredentials(Prob2.class.getResourceAsStream("/AwsCredentials.properties")));

        String bucketName = "";
        boolean bucketExists = false;
        
        // Expecting bucketName as the first argument 
        if (args.length > 0) {
    		bucketName = args[0];
    	
	        try {       
	            // Look for existing bucket with name matching args[0]
	            for (Bucket bucket : s3.listBuckets()) {
	            	if(bucketName.equals(bucket.getName())){
	            		bucketExists = true;
	            		System.out.println("Found bucket with matching name: " + bucket.getName());
	            	}              
	            }
	            
	            // only if read in argument == an existing bucket, then starting deleting things
	            if(bucketExists){	            
		            System.out.println("\nDeleting all objects in bucket:");
		            // List objects in found bucket
		            ObjectListing objectListing = s3.listObjects(new ListObjectsRequest().withBucketName(bucketName));
		            for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
		                System.out.println("Deleting object: "+objectSummary.getKey() + " (size = " + objectSummary.getSize() + ")");
		                // Delete all items found in bucket
		                s3.deleteObject(bucketName, objectSummary.getKey());
		            }
		            
		            // Delete bucket - which should now be empty
		            System.out.println("\nDeleting bucket " + bucketName + "\n");
		            s3.deleteBucket(bucketName);
	            }else{
	            	// no bucket with name taken from args exists
	            	System.out.println("No bucket with entered name: " + bucketName);
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
        }else{
        	System.err.println("Please enter some args!!!\nNote: First arg should be name of bucket to delete.\nCorrect usage is: java Classname bucketName");
        }
    }
}

package com.fjcx.e175;

import java.io.File;
import java.io.IOException;
import com.amazonaws.auth.*;
import com.amazonaws.services.s3.*;
import com.amazonaws.services.simpledb.*;
import com.amazonaws.services.simpledb.model.*;
import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.Principal;
import com.amazonaws.auth.policy.Policy;
import com.amazonaws.auth.policy.actions.S3Actions;
import com.amazonaws.auth.policy.resources.S3ObjectResource;

/**
 * E-175 Cloud Computing
 * Assignment 5
 * Problem 3 Part 2
 * Student: Frank O'Connor
 * Date: 10/08/12
 */

public class PersonRegistry {    
    public static final String s3BucketName = "personregistry-fjcx";
    public String simpleDBDomainName = "personRegistry";
    
    private AmazonS3 s3;
    private AmazonSimpleDB sdb;

    /**
     * Class constructor. Creates credential, S3Client and simpleDBClient objects
     */
    public PersonRegistry (){
        AWSCredentials credentials = null;
        
        try {
        	credentials = new PropertiesCredentials(PersonRegistry.class.getResourceAsStream("/AwsCredentials.properties"));
        }catch (IOException e){
        	System.out.println("We have issues with Credentials entered into AwsCredentials.properties.");
            System.out.println(e.getMessage());
            System.exit(-1);
        }

        // Create Amazon S3 and SimpleDB Clients
        this.s3  = new AmazonS3Client(credentials);
        this.sdb = new AmazonSimpleDBClient(credentials);           	
    }

    /**
     * This method creates S3 bucket, simpleDB domain and sets policies on bucket which
     * enables all users to have read access of items in bucket.
     */
    public void setupStorage(){
    	CreateDomainRequest createPersonDomainReq;
    	
    	// Create Person domain in SimpleDB.
        createPersonDomainReq = (new CreateDomainRequest()).withDomainName(simpleDBDomainName);
        this.sdb.createDomain(createPersonDomainReq);

        // Create the Amazon S3 bucket for person photos
        this.s3.createBucket(s3BucketName);
        System.out.println("Creating bucket " + s3BucketName + "\n");
        
        // Create Policy Statements		- *** Note: allowing public Read. Assuming it is not a good idea here to allow public write and assume it is not required here,
        // but do so would create another policy allowRestrictedWriteStatement with appropriate access for S3Actions.PutObject ***
        Statement allowPublicReadStatement = new Statement(Statement.Effect.Allow).withPrincipals(Principal.AllUsers)
        .withActions(S3Actions.GetObject).withResources(new S3ObjectResource(s3BucketName, "*"));
        System.out.println("Creating public read policy\n");

        // Attach Statements to a Policy
        Policy policy = new Policy().withStatements(allowPublicReadStatement);
        
        // Attach Policy to the bucket
        s3.setBucketPolicy(s3BucketName, policy.toJson());
        System.out.println("Attaching policy to S3 bucket\n");
    }
    
    // Decided to have to separate functions (rather than a generic class that takes any group, stars/nobels/other, and a would have used groupType to differentiate), 
    // as this stops users from altering/breaking the paths/keys. Also simpler for api user to enter only relevant parameters when parameters are specific to type of person
    /**
     * This method registers a movie star to the person registry. It does this by pushing appropriate items to 
     * S3 bucket with star appropriate file paths beginning with "stars/*". Then adds movie star relevant meta info to simpleDB 
     * @param personFirstName First name of movie Star.
     * @param personLastName Last name of movie Star.
     * @param picturefile File path for image of movie star.
     * @param resume File path for resume of movie star.
     * @param mostPopMovie Name of movie star's most popular movie.
     */
    public void registerStar(String personFirstName, String personLastName, String picturefile, String resume, String mostPopMovie) {  
    	String personName = personFirstName+"-"+personLastName;
    	String pictureS3Key = "stars/images/"+ personName +".jpg";
    	String resumeS3Key = "stars/resumes/"+ personName +".txt";
    	System.out.println("Registering info for Movie Star: "+ personFirstName + " "+ personLastName);
    
        // Store the picture in Amazon S3
        this.s3.putObject(s3BucketName, pictureS3Key, new File(picturefile));
        System.out.println("Storing image in S3 for "+personName);
        
        // Store the resume in Amazon S3
        this.s3.putObject(s3BucketName, resumeS3Key, new File(resume));
        System.out.println("Storing resume in S3 for "+personName);
        
        // Send the person information to Amazon SimpleDB
        PutAttributesRequest personAttributesReq = new PutAttributesRequest().withDomainName(simpleDBDomainName).withItemName(personName);
        System.out.println("Creating PutAttributeRequest for SimpleDB domain: "+simpleDBDomainName);

        // Add the person name as an attribute
        personAttributesReq.getAttributes().add(new ReplaceableAttribute().withName("FullName").withValue(personName).withReplace(true));        
        personAttributesReq.getAttributes().add(new ReplaceableAttribute().withName("MostPopMovie").withValue(mostPopMovie).withReplace(true));        
        personAttributesReq.getAttributes().add(new ReplaceableAttribute().withName("PictureS3Key").withValue(pictureS3Key).withReplace(true));        
        personAttributesReq.getAttributes().add(new ReplaceableAttribute().withName("ResumeS3Key").withValue(resumeS3Key).withReplace(true));
        System.out.println("Adding attributes to SimpleDB domain "+simpleDBDomainName+". Attributes-- "+"FullName:"+personName+", MostPopMovie:"+mostPopMovie+
        		", PictureS3Key:"+pictureS3Key+", ResumeS3Key:"+resumeS3Key );
        
        // Send the put attributes request
        this.sdb.putAttributes(personAttributesReq);
    }
    
    /**
     * This method registers a nobel prize winner to the person registry. It does this by pushing appropriate items to 
     * S3 bucket with nobel winner appropriate file paths beginning with "nobels/*". Then adds nobel prize winner relevant meta info to simpleDB 
     * @param personFirstName First name of nobel prize winner.
     * @param personLastName Last name of nobel prize winner.
     * @param picturefile File path for image of nobel prize winner.
     * @param resume File path for resume of nobel prize winner.
     * @param prizeYear Year the nobel prize winner won the nobel prize.
     * @param prizeField Name of field nobel prize winner won nobel prize in.
     */
    public void registerNobel(String personFirstName, String personLastName, String picturefile, String resume, String prizeYear, String prizeField) {  
    	String personName = personFirstName+"-"+personLastName;
    	String pictureS3Key = "nobels/images/"+ personName +".jpg";
    	String resumeS3Key = "nobels/resumes/"+ personName +".txt";
    	System.out.println("Registering info for Nobel prize winner: "+ personFirstName + " "+ personLastName);
    
        // Store the picture in Amazon S3
        this.s3.putObject(s3BucketName, pictureS3Key, new File(picturefile));
        System.out.println("Storing image in S3 for "+personName);
        
        // Store the resume in Amazon S3
        this.s3.putObject(s3BucketName, resumeS3Key, new File(resume));
        System.out.println("Storing resume in S3 for "+personName);
        
        // Send the person information to Amazon SimpleDB
        PutAttributesRequest personAttributesReq = new PutAttributesRequest().withDomainName(simpleDBDomainName).withItemName(personName);
        System.out.println("Creating PutAttributeRequest for SimpleDB domain: "+simpleDBDomainName);

        // Add the person name as an attribute
        personAttributesReq.getAttributes().add(new ReplaceableAttribute().withName("FullName").withValue(personName).withReplace(true));        
        personAttributesReq.getAttributes().add(new ReplaceableAttribute().withName("PrizeYear").withValue(prizeYear).withReplace(true));       
        personAttributesReq.getAttributes().add(new ReplaceableAttribute().withName("PrizeField").withValue(prizeField).withReplace(true));      
        personAttributesReq.getAttributes().add(new ReplaceableAttribute().withName("PictureS3Key").withValue(pictureS3Key).withReplace(true));       
        personAttributesReq.getAttributes().add(new ReplaceableAttribute().withName("ResumeS3Key").withValue(resumeS3Key).withReplace(true));
        System.out.println("Adding attributes to SimpleDB domain "+simpleDBDomainName+". Attributes-- "+"FullName:"+personName+", PrizeYear:"+prizeYear+
        		", PrizeField:"+prizeField+", PictureS3Key:"+pictureS3Key+", ResumeS3Key:"+resumeS3Key );     
        
        // Send the put attributes request
        this.sdb.putAttributes(personAttributesReq);
    }
}

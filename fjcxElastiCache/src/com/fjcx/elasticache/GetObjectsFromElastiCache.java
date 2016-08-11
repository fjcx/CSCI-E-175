package com.fjcx.elasticache;  
  
import java.net.InetSocketAddress; 
import java.util.HashMap;
import java.util.Date;  
  
import net.spy.memcached.MemcachedClient;	// This is the AWS-provided library with Auto Discovery support 
  

/**
 * E-175 Cloud Computing
 * Assignment 13
 * SetObjectsInElastiCache
 * Student: Frank O'Connor
 * Date: 18/12/12
 *
 * Class connects to Amazon ElastiCache cluster, creates some test objects and caches them
 */
public class GetObjectsFromElastiCache
{  
    public GetObjectsFromElastiCache() throws Exception  
    {     
	}
	
	public void getObjectsFromCache() throws Exception  { 
		
		// setting configuration endpoint of cluster
		String configEndpoint = "fjcxcachecluster1.rxwojn.cfg.use1.cache.amazonaws.com:11200";
        Integer clusterPort = 11211;
		
        // connecting to memcahced client
		MemcachedClient memcacheClient = new MemcachedClient(new InetSocketAddress(configEndpoint, clusterPort));
        // fetching test objects from cache
		HashMap<?, ?> fetchedCacheObject = (HashMap<?, ?>)memcacheClient.get("fjcxTestObject");
         
        String testvals = (String)fetchedCacheObject.get("TestVal1");  
        if (testvals != null) {  
        	System.out.println("fetched values from cached hashmap object:" + fetchedCacheObject);
        } 
        testvals = (String)fetchedCacheObject.get("TestVal2");  
        if (testvals != null) {  
        	System.out.println("fetched values from cached hashmap object:" + fetchedCacheObject);
        }  
        
        Date fetchedCacheDate = (Date)memcacheClient.get("fjcxTestDate");
        System.out.println("fetched cached date:" + fetchedCacheDate);
    }  
}  
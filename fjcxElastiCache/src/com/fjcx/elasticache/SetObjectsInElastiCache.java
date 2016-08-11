package com.fjcx.elasticache;

import java.net.InetSocketAddress;
import java.util.Date;
import java.util.HashMap;

import net.spy.memcached.MemcachedClient;	// This is the AWS-provided library with Auto Discovery support 

/**
 * E-175 Cloud Computing
 * Assignment 13
 * SetObjectsInElastiCache
 * Student: Frank O'Connor
 * Date: 18/12/12
 *
 * Class connects to Amazon ElastiCache cluster, fetches test object from the cache 
 */
public class SetObjectsInElastiCache
{
	public void setObjectsInCache() throws Exception{
		
		// setting configuration endpoint of cluster
		String configEndpoint = "fjcxcachecluster1.rxwojn.cfg.use1.cache.amazonaws.com:11200";
        Integer clusterPort = 11211;
		
        // connecting to memcahced client
		MemcachedClient memcacheClient = new MemcachedClient(new InetSocketAddress(configEndpoint, clusterPort));
		// create test object to cache
		HashMap<String, String> testMapToCache = new HashMap<String, String>(); 
		testMapToCache.put("TestVal1", "ElastiTest1"); 
		testMapToCache.put("TestVal2", "ElastiTest2"); 
		
		// cache our test objects
		memcacheClient.set("fjcxTestObject", 3600, testMapToCache);
		
		Date todayDate = new Date();
		memcacheClient.set("fjcxTestDate", 3600, todayDate);
		
	}
}





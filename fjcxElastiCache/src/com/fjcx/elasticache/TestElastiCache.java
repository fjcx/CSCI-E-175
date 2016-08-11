package com.fjcx.elasticache;  
 
/**
 * E-175 Cloud Computing
 * Assignment 13
 * TestElastiCache
 * Student: Frank O'Connor
 * Date: 18/12/12
 *
 * Class connects to Amazon ElastiCache cluster, creates some test objects and 
 * then fetches object again from the cache to test basic interaction from java client 
 */
public class TestElastiCache
{  
    public static void main(String[] args) throws Exception  
    {  
    	// creating test objects and storing them in cache
    	SetObjectsInElastiCache setTest = new SetObjectsInElastiCache();
    	setTest.setObjectsInCache();
    	
    	// fetching test objects from storage in cache
    	GetObjectsFromElastiCache getTest = new GetObjectsFromElastiCache();
        getTest.getObjectsFromCache();
        
    }  
}  
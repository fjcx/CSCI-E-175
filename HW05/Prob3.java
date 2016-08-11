package com.fjcx.e175;

/**
 * E-175 Cloud Computing
 * Assignment 5
 * Problem 3 Part 1
 * Student: Frank O'Connor
 * Date: 10/08/12
 *
 * Class creates instance of PersonRegistry which is then used to store info for both movie stars and nobel prize winners  
 */
public class Prob3 {
	
	public static void main(String[] args) {
		PersonRegistry personRegistry = new PersonRegistry();
		
		// Initializes PersonRegistry instance. Creates S3 bucket and simpleDB domain.
		personRegistry.setupStorage();
		
		// Stores info for movie stars. It does so by creating objects in S3 bucket and by storing meta info in simpleDB domain 
		personRegistry.registerStar("Liam", "Neeson", "readPeople/liamNeeson1.jpg", "readPeople/liamNeesonResume.txt", "SchindlersList");
		personRegistry.registerStar("Daniel", "DayLewis", "readPeople/danielDayLewis1.jpg", "readPeople/danielDayLewisResume.txt", "MyLeftFoot");
		
		// Stores info for nobel prize winner. It does so by creating objects in S3 bucket and by storing meta info in simpleDB domain 
		personRegistry.registerNobel("Earnest", "Walton", "readPeople/ernestWalton1.jpg", "readPeople/ernestWaltonResume.txt", "1951", "Physics");
		personRegistry.registerNobel("Seamus", "Heaney", "readPeople/seamusHeaney1.jpg", "readPeople/seamusHeaneyResume.txt", "1995", "Literature");
	}
}

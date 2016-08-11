package com.rest.client;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

public class CustomerResourceClient {
	public static void main(String[] args) {
		try {
			System.out.println("*** Create a new Customer ***");
			// Create a new customer
			String newCustomer = "<customer>" + "<first-name>Bill</first-name>" + "<last-name>Burke</last-name>"
					+ "<street>256 Clarendon Street</street>" + "<city>Boston</city>" + "<state>MA</state>"
					+ "<zip>02115</zip>" + "<country>USA</country>" + "</customer>";

			URL postUrl = new URL("http://localhost:8090/rest/customers");
			HttpURLConnection connection = (HttpURLConnection) postUrl.openConnection();
			System.out.println("opened connection...");
			connection.setDoOutput(true);
			connection.setInstanceFollowRedirects(false);
			connection.setRequestMethod("POST");
			connection.setRequestProperty("Content-Type", "application/xml");
			OutputStream os = connection.getOutputStream();
			os.write(newCustomer.getBytes());
			os.flush();
			System.out.println(HttpURLConnection.HTTP_CREATED);
			System.out.println(connection.getResponseCode());
			System.out.println("Location: " + connection.getHeaderField("Location"));
			connection.disconnect();

			// Get the new customer
			System.out.println("*** GET Created Customer **");
			URL getUrl = new URL("http://localhost:8090/rest/customers/1");
			connection = (HttpURLConnection) getUrl.openConnection();
			connection.setRequestMethod("GET");
			System.out.println("Content-Type: " + connection.getContentType());

			BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));

			String line = reader.readLine();
			while (line != null) {
				System.out.println(line);
				line = reader.readLine();
			}
			System.out.println(HttpURLConnection.HTTP_OK);
			System.out.println(connection.getResponseCode());
			connection.disconnect();

			// Update the new customer. Change Bill's name to William
			String updateCustomer = "<customer>" + "<first-name>William</first-name>" + "<last-name>Burke</last-name>"
					+ "<street>256 Clarendon Street</street>" + "<city>Boston</city>" + "<state>MA</state>"
					+ "<zip>02115</zip>" + "<country>USA</country>" + "</customer>";
			connection = (HttpURLConnection) getUrl.openConnection();
			connection.setDoOutput(true);
			connection.setRequestMethod("PUT");
			connection.setRequestProperty("Content-Type", "application/xml");
			os = connection.getOutputStream();
			os.write(updateCustomer.getBytes());
			os.flush();
			System.out.println(HttpURLConnection.HTTP_NO_CONTENT);
			System.out.println(connection.getResponseCode());
			connection.disconnect();
			
			// Get the updated customer
			System.out.println("*** GET Updated Customer **");
			getUrl = new URL("http://localhost:8090/rest/customers/1");
			connection = (HttpURLConnection) getUrl.openConnection();
			connection.setRequestMethod("GET");
			System.out.println("Content-Type: " + connection.getContentType());

			reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));

			line = reader.readLine();
			while (line != null) {
				System.out.println(line);
				line = reader.readLine();
			}
			System.out.println(HttpURLConnection.HTTP_OK);
			System.out.println(connection.getResponseCode());
			connection.disconnect();

			
			/* changes */
			// Create 4 new customers
			for(int i=2; i<6;i++){
				newCustomer = "<customer>" + "<first-name>Bill-"+i+"</first-name>" + "<last-name>Burke-"+i+"</last-name>"
						+ "<street>256 Clarendon Street</street>" + "<city>Boston</city>" + "<state>MA</state>"
						+ "<zip>02115</zip>" + "<country>USA</country>" + "</customer>";

				postUrl = new URL("http://localhost:8090/rest/customers");
				connection = (HttpURLConnection) postUrl.openConnection();
				System.out.println("opened connection...");
				connection.setDoOutput(true);
				connection.setInstanceFollowRedirects(false);
				connection.setRequestMethod("POST");
				connection.setRequestProperty("Content-Type", "application/xml");
				os = connection.getOutputStream();
				os.write(newCustomer.getBytes());
				os.flush();
				System.out.println(HttpURLConnection.HTTP_CREATED);
				System.out.println(connection.getResponseCode());
				System.out.println("Location: " + connection.getHeaderField("Location"));
				connection.disconnect();
			}
			
			// Get customers 2-4
			System.out.println("*** GET Customers 2 - 4 **");
			getUrl = new URL("http://localhost:8090/rest/customers?start=2&size=4");
			connection = (HttpURLConnection) getUrl.openConnection();
			connection.setRequestMethod("GET");
			System.out.println("Content-Type: " + connection.getContentType());

			reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));

			line = reader.readLine();
			while (line != null) {
				System.out.println(line);
				line = reader.readLine();
			}
			System.out.println(HttpURLConnection.HTTP_OK);
			System.out.println(connection.getResponseCode());
			connection.disconnect();


			// Delete a customer
			System.out.println("*** Deleted Customer **");
			getUrl = new URL("http://localhost:8090/rest/customers/1");
			connection = (HttpURLConnection) getUrl.openConnection();
			connection.setRequestMethod("DELETE");
			System.out.println("Content-Type: " + connection.getContentType());

			reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));

			line = reader.readLine();
			while (line != null) {
				System.out.println(line);
				line = reader.readLine();
			}
			System.out.println(HttpURLConnection.HTTP_OK);
			System.out.println(connection.getResponseCode());
			connection.disconnect();
						
			// Attempt to get the deleted customer
			System.out.println("*** Attempt to GET Deleted Customer **");
			getUrl = new URL("http://localhost:8090/rest/customers/1");
			connection = (HttpURLConnection) getUrl.openConnection();
			connection.setRequestMethod("GET");
			System.out.println("Content-Type: " + connection.getContentType());

			reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));

			line = reader.readLine();
			while (line != null) {
				System.out.println(line);
				line = reader.readLine();
			}
			System.out.println(HttpURLConnection.HTTP_OK);
			System.out.println(connection.getResponseCode());
			connection.disconnect();
			


		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

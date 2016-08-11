package com.rest.client;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import javax.ws.rs.core.MediaType;

import java.net.HttpURLConnection;


public class CustomerResourceClient4 {
	public static void main(String[] args) {
		try {
			SimpleResourceClient erc = new SimpleResourceClient();
			System.out.println("*** Create a new Customer ***");
			// Create a new customer
			String newCustomer = "<customer>" + "<first-name>Bill</first-name>" + "<last-name>Burke</last-name>"
					+ "<street>256 Clarendon Street</street>" + "<city>Boston</city>" + "<state>MA</state>"
					+ "<zip>02115</zip>" + "<country>USA</country>" + "</customer>";
			
			ClientResponse response = erc.postCustomer(newCustomer);
			if (response.getStatus() != 201) {
				throw new RuntimeException("Failed : HTTP error code : " + response.getStatus());
			}
			System.out.println(HttpURLConnection.HTTP_CREATED);
			System.out.println(response.getStatus());
			System.out.println("Location: " + response.getHeaders().get("Location"));
			
			// Get the new customer
			System.out.println("\n*** GET Created Customer **");
			response = erc.getCustomer(1);
			
			if (response.getStatus() != 200) {
				   throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());
			}
			System.out.println(response.getEntity(String.class));
			System.out.println(HttpURLConnection.HTTP_OK);
			System.out.println(response.getStatus());

			System.out.println("\n*** Updating Customer **");
			// Update the new customer. Change Bill's name to William
			String updateCustomer = "<customer>" + "<first-name>William</first-name>" + "<last-name>Burke</last-name>"
					+ "<street>256 Clarendon Street</street>" + "<city>Boston</city>" + "<state>MA</state>"
					+ "<zip>02115</zip>" + "<country>USA</country>" + "</customer>";
			erc.putCustomer(1, updateCustomer);
			System.out.println(HttpURLConnection.HTTP_NO_CONTENT);
			System.out.println(response.getStatus());
			
			// Get the new customer
			System.out.println("\n*** GET Created Customer **");
			response = erc.getCustomer(1);
			
			if (response.getStatus() != 200) {
				   throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());
			}
			System.out.println(response.getEntity(String.class));
			System.out.println(HttpURLConnection.HTTP_OK);
			System.out.println(response.getStatus());
						
	        erc.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	static class SimpleResourceClient {

        private Client client;
        private static final String BASE_URI = "http://localhost:8090/rest/customers";

        public SimpleResourceClient() {
            ClientConfig config = new DefaultClientConfig();
            client = Client.create(config);
        }

        public ClientResponse postCustomer(String newCustomer) throws UniformInterfaceException {        	
            WebResource resource = client.resource(BASE_URI);
            return resource.type(MediaType.APPLICATION_XML).post(ClientResponse.class, newCustomer);
        }
        
        public ClientResponse getCustomer(int custId) throws UniformInterfaceException {
            WebResource resource = client.resource(BASE_URI+"/"+custId);
            return resource.accept(MediaType.APPLICATION_XML).get(ClientResponse.class);
        }

        public ClientResponse putCustomer(int custId, String updateCustomer) throws UniformInterfaceException {
        	WebResource resource = client.resource(BASE_URI+"/"+custId);
            return resource.type(MediaType.APPLICATION_XML).put(ClientResponse.class, updateCustomer);
        }

        public void close() {
            client.destroy();
        }
    }
}

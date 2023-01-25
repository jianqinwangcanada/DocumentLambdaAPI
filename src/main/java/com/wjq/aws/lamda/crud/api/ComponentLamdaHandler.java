package com.wjq.aws.lamda.crud.api;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
//import java.util.HashMap;
import java.util.Iterator;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
//import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
//import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
//import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
//import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
//import com.amazonaws.services.dynamodbv2.xspec.ScanExpressionSpec;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.wjq.aws.lamda.crud.api.model.Component;
//import com.amazonaws.services.dynamodbv2.document.RangeKeyCondition;
public class ComponentLamdaHandler implements RequestStreamHandler {
   private String DYNAMO_TABLE = "ComponentTable";
	@SuppressWarnings("unchecked")
	@Override
	public void handleRequest(InputStream input, OutputStream output, Context context) throws IOException {
		OutputStreamWriter writer = new OutputStreamWriter(output);
		BufferedReader reader = new BufferedReader(new InputStreamReader(input));
		JSONParser parser = new JSONParser(); // this will help us parse the request object
		JSONObject responseObject = new JSONObject(); // we will add to this object for our api response
		JSONObject responseBody = new JSONObject();// we will add the item to this object
		
		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.defaultClient();
		DynamoDB dynamoDB = new DynamoDB(client);
		
		int id; 
		Item resItem = null;
		
//		RangeKeyCondition condition = new RangeKeyCondition("id").between(1,3);
       
		try {
			JSONObject reqObject = (JSONObject) parser.parse(reader);
			
			//pathParameters
			if (reqObject.get("pathParameters")!=null) {
				JSONObject pps = (JSONObject)reqObject.get("pathParameters");
				if (pps.get("id")!=null) {
					id = Integer.parseInt((String)pps.get("id"));
					resItem = dynamoDB.getTable(DYNAMO_TABLE).getItem("id",id);
				}
			}
			//queryStringParameters
			else if (reqObject.get("queryStringParameters")!=null) {
				JSONObject qps =(JSONObject) reqObject.get("queryStringParameters");
				if (qps.get("id")!=null) {
					id= Integer.parseInt((String)qps.get("id"));
					resItem = dynamoDB.getTable(DYNAMO_TABLE).getItem("id",id);
					
				}
			}
			
			if (resItem!=null) {
				Component product = new Component(resItem.toJSON());
				responseBody.put("component", product);
				responseObject.put("statusCode", 200);
			}else {
				responseBody.put("message", "No Items Found");
				responseObject.put("statusCode", 404);
			}
			
			responseObject.put("body", responseBody.toString());
			
		} catch (Exception e) {
			context.getLogger().log("ERROR : "+e.getMessage());
		}
		writer.write(responseObject.toString());
		reader.close();
		writer.close();
	
	}
	
	@SuppressWarnings("unchecked")
	public void handlePutRequest(InputStream input, OutputStream output, Context context) throws IOException {
		OutputStreamWriter writer = new OutputStreamWriter(output);
		BufferedReader reader = new BufferedReader(new InputStreamReader(input));
		JSONParser parser = new JSONParser(); // this will help us parse the request object
		JSONObject responseObject = new JSONObject(); // we will add to this object for our api response
		JSONObject responseBody = new JSONObject();// we will add the item to this object
		
		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.defaultClient();
		DynamoDB dynamoDB = new DynamoDB(client);
		
		try {
			JSONObject reqObject =(JSONObject) parser.parse(reader);
			
			if (reqObject.get("body")!=null) {
				Component component = new Component((String)reqObject.get("body"));
				
				dynamoDB.getTable(DYNAMO_TABLE)
				.putItem(new PutItemSpec().withItem(new Item()
						.withNumber("id", component.getId())
						.withString("name", component.getName())
						.withString("description", component.getDescription())));
				responseBody.put("message", "New Item created/updated");
				responseObject.put("statusCode", 200);
				responseObject.put("body", responseBody.toString());
			}
			
			
		} catch (Exception e) {
			responseObject.put("statusCode", 400);
			responseObject.put("error", e);
		}
		
		writer.write(responseObject.toString());
		reader.close();
		writer.close();
	}
	
	
	@SuppressWarnings("unchecked")
	public void handleDeleteRequest(InputStream input, OutputStream output, Context context) throws IOException {
		OutputStreamWriter writer = new OutputStreamWriter(output);
		BufferedReader reader = new BufferedReader(new InputStreamReader(input));
		JSONParser parser = new JSONParser(); // this will help us parse the request object
		JSONObject responseObject = new JSONObject(); // we will add to this object for our api response
		JSONObject responseBody = new JSONObject();// we will add the item to this object
		
		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.defaultClient();
		DynamoDB dynamoDB = new DynamoDB(client);
		
		try {
			JSONObject reqObject =(JSONObject) parser.parse(reader);
			
			if (reqObject.get("pathParameters")!=null) {
				JSONObject pps =(JSONObject) reqObject.get("pathParameters");
				
				if (pps.get("id")!=null) {
					int id = Integer.parseInt((String)pps.get("id"));
					dynamoDB.getTable(DYNAMO_TABLE).deleteItem("id",id);
				}
				
			}
			
			responseBody.put("message", "Item deleted");
			responseObject.put("statusCode", 200);
			responseObject.put("body", responseBody.toString());
			
			
		} catch (Exception e) {
			responseObject.put("statusCode", 400);
			responseObject.put("error", e);
		}
		
		writer.write(responseObject.toString());
		reader.close();
		writer.close();
	}
	@SuppressWarnings("unchecked")
	
	public void handleConditionRequest(InputStream input, OutputStream output, Context context) throws IOException {
		OutputStreamWriter writer = new OutputStreamWriter(output);
		BufferedReader reader = new BufferedReader(new InputStreamReader(input));
		JSONParser parser = new JSONParser(); // this will help us parse the request object
		JSONObject responseObject = new JSONObject(); // we will add to this object for our api response
		JSONObject responseBody = new JSONObject();// we will add the item to this object
		
		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.defaultClient();
		DynamoDB dynamoDB = new DynamoDB(client);
		String str="";

		try {
			JSONObject reqObject =(JSONObject) parser.parse(reader);
			if (reqObject.get("queryStringParameters")!=null) {
				JSONObject pps =(JSONObject) reqObject.get("queryStringParameters");
				if (pps.get("v1")!=null && pps.get("v2")!=null) {
					int v1 = Integer.parseInt((String)pps.get("v1"));
					int v2 = Integer.parseInt((String)pps.get("v2"));
//   **********************Can only get one item
//					QuerySpec querySpec = new QuerySpec()							
//				            .withProjectionExpression("id, #nm,description")
//				            .withKeyConditionExpression("id = :v_id")
//				            .withNameMap(new NameMap().with("#nm", "name"))
//				            .withValueMap(new ValueMap().withNumber(":v_id", 1));
				           // .withFilterExpression("id > :vid")
				           
			 ScanSpec scanSpec = new ScanSpec()							
            .withProjectionExpression("id, #nm,description")
           // .withKeyConditionExpression("id = :v_id")
            .withFilterExpression("id between :v1 and :v2")
            .withNameMap(new NameMap().with("#nm", "name"))
            .withValueMap(new ValueMap().withNumber(":v1", v1).withNumber(":v2", v2));
			
		            ItemCollection<ScanOutcome> items = dynamoDB.getTable(DYNAMO_TABLE).scan(scanSpec);
		           // Component item1 = items.
		           Iterator<Item> iterator = items.iterator();
		           Item item = null;

		            while(iterator.hasNext()) {
		            	item=iterator.next();
		            	str+=item.toJSONPretty();
		            }
				
				}
		            responseBody.put("message", str);
					responseObject.put("statusCode", 200);
					responseObject.put("body", responseBody.toString());
				
			}
			
			
		} catch (Exception e) {
			responseObject.put("statusCode", 400);
			responseObject.put("error", e);
		}
		writer.write(responseObject.toString());
		reader.close();
		writer.close();
	}
	
	

}



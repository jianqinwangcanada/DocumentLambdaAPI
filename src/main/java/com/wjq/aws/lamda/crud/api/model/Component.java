package com.wjq.aws.lamda.crud.api.model;

import com.google.gson.Gson;

public class Component {
	private int id;
	private String name;
	private String description;
	
	public Component(int id,String name,String description) {
		this.id =id;
		this.name = name;
		this.description = description;
	}
	
	//Create object from json
	public Component(String json) {
		Gson gson = new Gson();
		Component tempComponent = gson.fromJson(json, Component.class);
		this.id = tempComponent.id;
		this.name = tempComponent.name;
		this.description = tempComponent.description;
	}
	public String toString() {
		return new Gson().toJson(this);
	}
	
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	
	

}

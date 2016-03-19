package com.netflix.metacat.common.partition.parser;

public class Variable {
	private String name;

	public Variable(String name) {
		this.setName(name);
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}

package io;

public class InvalidDateException extends Exception {

	private static final long serialVersionUID = 1L;

	public InvalidDateException(String value) {
		super("Invalid date: " + value);
	}
	
}

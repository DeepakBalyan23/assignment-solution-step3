package com.stackroute.datamunger.reader;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import com.stackroute.datamunger.query.DataTypeDefinitions;
import com.stackroute.datamunger.query.Header;

public class CsvQueryProcessor extends QueryProcessingEngine {

	private String fileName;
	// Parameterized constructor to initialize filename
	public CsvQueryProcessor(String fileName) throws FileNotFoundException {
		this.fileName=fileName;
	}

	/*
	 * Implementation of getHeader() method. We will have to extract the headers
	 * from the first line of the file.
	 * Note: Return type of the method will be Header
	 */
	
	@Override
	public Header getHeader() throws IOException {

		// read the first line
		FileReader fileReader = new FileReader(fileName);
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		// populate the header object with the String array containing the header names
		Header header =new Header();
		header.setHeaders(bufferedReader.readLine().split(","));
		bufferedReader.close();
		return header;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	/**
	 * getDataRow() method will be used in the upcoming assignments
	 */
	
	@Override
	public void getDataRow() {

	}

	/*
	 * Implementation of getColumnType() method. To find out the data types, we will
	 * read the first line from the file and extract the field values from it. If a
	 * specific field value can be converted to Integer, the data type of that field
	 * will contain "java.lang.Integer", otherwise if it can be converted to Double,
	 * then the data type of that field will contain "java.lang.Double", otherwise,
	 * the field is to be treated as String. 
	 * Note: Return Type of the method will be DataTypeDefinitions
	 */
	
	@Override
	public DataTypeDefinitions getColumnType() throws IOException {
		DataTypeDefinitions dataTypeDefinitions = new DataTypeDefinitions();
		FileReader fileReader = new FileReader(fileName);
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		int headerLength = bufferedReader.readLine().split(",").length;
		String[] fields = bufferedReader.readLine().split(",", headerLength);
		bufferedReader.close();
		String[] dataTypes = new String[headerLength];
		int index=0;
		for(String field: fields) {
			try {
				Integer i = Integer.parseInt(field);
				dataTypes[index++]=i.getClass().getName();
			} catch(NumberFormatException e) {
				try {
					Double d = Double.parseDouble(field);
					dataTypes[index++]=d.getClass().getName();
					System.out.println(e);
				} catch(NumberFormatException ee) {
					dataTypes[index++]=field.getClass().getName();
					System.out.println(ee);
				}
			}
		}
		dataTypeDefinitions.setDataTypes(dataTypes);
		return dataTypeDefinitions;
	}
}

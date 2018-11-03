package com.acme.quicktest;

import java.io.File;
import java.io.IOException;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import java.util.Date;
import java.util.List;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;

/**
 * 
 * Thread that processes Text files e.g. SAS/Java/SQL/Text file extensions
 * 
 * @author kulkarvi
 *
 */
public class ProcessFileThread implements Runnable {
	private File file;
	private static Logger logger = Logger.getLogger(ProcessFileThread.class.getName());
	
	/**
	 * 
	 * @param file
	 */
	public ProcessFileThread(File file) {
		this.file = file;
		logger.trace("file:" + file);
	}
	
	
	/**
	 * 
	 */
	public void run() {
		logger.info("Started reading file: " + file.getName() + " at time: " + new Date());

        try {
        	pushToMessageQueue("testQueue");
        	printStatistics();
        	
        	// Uncomment to print the contents of the file
        	//printFile();
			
		} 
        catch (Exception e) {
			e.printStackTrace();
		} 

		logger.info("Finished Reading file: " + file + " at time: " + new Date());
	}

	
	/**
	 * 
	 * @param queueName
	 */
	private void pushToMessageQueue(String queueName) {
		logger.info("pushing File: " + file.getName() + " to Message Queue: " + queueName);
		
		//TODO: pushing file contents to message queue - implemented here
		logger.debug("done !!!");
	}
	
	
	/**
	 * Print the lines count, word count for the file
	 * @throws IOException 
	 * 
	 */
	private void printStatistics() throws IOException {
	    String fileExtension = FilenameUtils.getExtension(file.getAbsolutePath());
	    logger.info("File extension: " + fileExtension);
	    
	    // Process files only having the following extensions
	    if ("sas".equalsIgnoreCase(fileExtension) || "java".equalsIgnoreCase(fileExtension) || 
	    		"sql".equalsIgnoreCase(fileExtension) || "txt".equalsIgnoreCase(fileExtension)) {
	    	
			List<String> lines = Files.readAllLines(Paths.get(file.getAbsolutePath()));
			long lineCount = Files.lines(Paths.get(file.getAbsolutePath())).count();
			
			long bytes = file.length( );
		    String size = FileUtils.byteCountToDisplaySize(bytes);
		    
			logger.info("------------------------------------------");
			logger.info("File Name: " + file.getName());
			logger.info("File size: " + size);
		    logger.info("Lines Count: " + lineCount);
		    logger.info("------------------------------------------");
		    
   	    	Stream<String> fileLines = Files.lines(Paths.get(file.getAbsolutePath()), Charset.defaultCharset());
   	    	
   		    // TODO: count the number of words
   		    //long wordCount = fileLines.map(line -> Arrays.stream(line.split(" "))).count();
	    }
	    else
	    	logger.info("Ignoring File: " + file.getName());

	}
	
	
	/**
	 * Print the file contents to console
	 * @throws IOException 
	 * 
	 */
	private void printFile() throws IOException {
	    String fileExtension = FilenameUtils.getExtension(file.getAbsolutePath());
	    
	    // Process files only having the following extensions
	    if ("sas".equalsIgnoreCase(fileExtension) || "java".equalsIgnoreCase(fileExtension) || 
	    		"sql".equalsIgnoreCase(fileExtension) || "txt".equalsIgnoreCase(fileExtension)) {
	    	
	    	List<String> lines = Files.readAllLines(Paths.get(file.getAbsolutePath()));
		
	    	for (String line : lines)
	    		System.out.println(line);
	    }
	}
}

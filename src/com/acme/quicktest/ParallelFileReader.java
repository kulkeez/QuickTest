package com.acme.quicktest;

import java.io.File;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

/**
 * 
 * Program that reads multiple files in a specified folder concurrently
 * 
 * @author kulkarvi
 *
 */
public class ParallelFileReader {

	private static Logger logger = Logger.getLogger(ParallelFileReader.class.getName());
	
	public static final int POOL_SIZE = 100;
	//public static final String FOLDER_OPTION = "C:\\\\Temp\\\\SAS";
	public static final String FOLDER_OPTION = "C:\\Users\\kulkarvi\\Downloads";
	
	/**
	 * 
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			BasicConfigurator.configure();
			logger.info("Launching the ParallelFileReader application ...");
			
		    HelpFormatter formatter = new HelpFormatter();
		    formatter.printHelp("ParallelFileReader", generateOptions());
		      
			//TODO: hard-coded, read file folder as input
			logger.info("Reading the following files in folder: " + FOLDER_OPTION);
			
			RejectedExecutionHandlerImpl rejectionHandler = new RejectedExecutionHandlerImpl();
			
			int cpuCoresCount = Runtime.getRuntime().availableProcessors();
            logger.info("File Separator = " + System.getProperty("file.separator"));
            logger.info("Temp Directory = " +  new File(System.getProperty("java.io.tmpdir")).getPath());
            logger.info("User Home Directory = " +  new File(System.getProperty("user.home")).getPath());
			logger.info("CPU cores available on this machine: " + cpuCoresCount);
            
			// Creating a fixed size thread pool of 15 worker threads. 
			ExecutorService fixedExecutorService = Executors.newFixedThreadPool(cpuCoresCount);
			logger.debug("Creating a fixed size thread pool of " + cpuCoresCount + " worker threads.");
			
			ProcessFileThread fileProcessor = null;
			File dir = new File(FOLDER_OPTION);
			
			// List all the files in the input folder
			listFiles(dir);
			
			for (File file : dir.listFiles()) {
				//TODO: add logic that identifies message file and launches appropriate thread flavours
				
				if(! file.isDirectory()) {
					fileProcessor = new ProcessFileThread(file);
					fixedExecutorService.submit(fileProcessor);
				}
            }
			
			fixedExecutorService.shutdown();
	        
			while (!fixedExecutorService.isTerminated()) {
	        }
	        logger.debug("Finished all worker threads");

        }
        catch (Exception e) {
            e.printStackTrace();
        }
	}

	
	/**
	 * 
	 * List the files being read concurrently 
	 * 
	 */
	private static void listFiles(File dir) {
		System.out.println("Listing the files to read...");
		System.out.println("------------------------------------");

		for (String name : dir.list())
			System.out.println(name);
		
		System.out.println("------------------------------------");
	}
	
	private static Options generateOptions() {
   
		final Options options = new Options();
		
/*	    options.addOption("f", "folder", true, "Folder from which to read files.")
	    	.addOption("h", "help", false, "Show help");*/

		final Option folderOption = Option.builder("f")
				.required()
				.longOpt(FOLDER_OPTION)
				.hasArg()
				.desc("Folder from which to read files.")
				.build();
		
		options.addOption(folderOption);
		return options;
	}	
	
}



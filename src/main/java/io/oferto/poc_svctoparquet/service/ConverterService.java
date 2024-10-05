package io.oferto.poc_svctoparquet.service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;


import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

@Service
public class ConverterService {
	private final String EXPORT_PATH = "/Users/miguel/temp/";
	
    public String convert(MultipartFile file) throws IOException {
    	Path tempFile = Files.createTempFile(null, null);
    	Files.write(tempFile, file.getBytes());
    			
    	SparkSession spark = SparkSession.builder()
    			  .appName("Java Spark csv to parquet poc")
    			  .master("local[*]")
    			  .config("spark.shuffle.blockTransferService", "nio")
    			  .getOrCreate();
    	
    	Dataset<Row> df = spark.read().format("csv")
    			.option("header", "true")
    			.option("delimiter", ",")
                .option("inferSchema", "true")
                .load(tempFile.toString());

    	df.write()
    	  .format("parquet")
    	  .save(EXPORT_PATH + file.getOriginalFilename() + ".parquet");
    	  
    	return "File convert successfully: " + file.getName() + ".parquet to " + EXPORT_PATH;
    }
}

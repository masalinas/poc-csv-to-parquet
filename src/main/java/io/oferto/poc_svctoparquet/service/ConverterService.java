package io.oferto.poc_svctoparquet.service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

@Service
public class ConverterService {
	private final String EXPORT_PATH = "/home/miguel/temp/";
	
    public String convert(MultipartFile file) throws IOException {
    	String tmpdir = System.getProperty("java.io.tmpdir");
        System.out.println("Temp file path: " + tmpdir);

        // Create SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("File Reader")
                .master("local[*]") // Use local mode with all available cores
                .getOrCreate();
        
        // create a temporal file
        Path tempFile = Files.createTempFile(null, null);
        System.out.println(tempFile);

        // write csv file bytes array to the temporal one
        Files.write(tempFile, file.getBytes());
        
        // read the CSV temporal file to Spark Dataframe
        Dataset<Row> fileData = spark.read().format("csv")
                .option("header", "true")      // If CSV has a header
                .option("inferSchema", "true") // Infer data types
                .load(tempFile.toString());   
                 
        // write Spark Dataframe to parquet file
        fileData.write()
        	.format("parquet")
        	.save(EXPORT_PATH + file.getName() + ".parquet");
            
        return "File convert successfully: " + file.getName() + ".parquet to " + EXPORT_PATH;
    }
}

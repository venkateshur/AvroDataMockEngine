package com.capitolone.mock.avro;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.apache.spark.sql.functions;
import org.apache.spark.storage.StorageLevel;


import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class FilesSelector {
    private static final String SPLITTER = ";";

    public static void main(String[] args) {

        String inputPath = args[0];
        String outputDir = args[1];

        String currentTimestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));

        SparkSession spark = SparkSession.builder().appName("AVRO MOCK ENGINE").getOrCreate();
        try {
            Dataset<Row> inAvro = spark.read().format("parquet").load(inputPath)
                    .withColumn("location", functions.input_file_name())
                    .withColumn("private_jon_key", functions.col("domainPayload.applicationId"))
                    .withColumn("private_join_key_type", functions.lit("application_id"));

            Dataset<Row> fileDetails = inAvro.select("private_jon_key", "location", "private_join_key_type").distinct();
            fileDetails.coalesce(1).write().option("header", "true").mode("overwrite").csv(outputDir + "/processed_files/" + currentTimestamp);
        } catch (Exception exception){
            exception.printStackTrace();
            System.out.println("Mock Avro App failed with error : " + exception.getMessage());
            System.exit(1);
        }
    }
}


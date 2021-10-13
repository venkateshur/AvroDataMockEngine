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

public class AvroDataMockEngine {
    private static final String SPLITTER = ";";

    public static void main(String[] args) {
        String recordsSelectionMode;

        String inputPath = args[0];
        List<Integer> requiredNumberOfRecordsArray;
        String outputDir = args[2];

        if (args.length == 3) {
            recordsSelectionMode = args[3];
        } else recordsSelectionMode = "RECORDS";

        String currentTimestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));

        SparkSession spark = SparkSession.builder().appName("AVRO MOCK ENGINE").getOrCreate();
        try {
            Dataset<Row> inAvro = spark.read().format("avro").load(inputPath)
                    .withColumn("location", functions.input_file_name())
                    .withColumn("private_jon_key", functions.col("domainPayload.applicationId"))
                    .withColumn("private_join_key_type", functions.lit("application_id"))
                    .persist(StorageLevel.MEMORY_AND_DISK());

            int avroInRecordsCount = Integer.parseInt(String.valueOf(inAvro.count()));

            if (Objects.equals(recordsSelectionMode, "PERCENTAGE")) {
                requiredNumberOfRecordsArray =
                        Arrays.stream(args[1].split(SPLITTER))
                                .map(per -> Math.abs(avroInRecordsCount * (Integer.parseInt(per) / 100))).collect(Collectors.toList());
            } else {
                requiredNumberOfRecordsArray = Arrays.stream(args[1].split(SPLITTER))
                        .map(Integer::parseInt).collect(Collectors.toList());
            }

            requiredNumberOfRecordsArray.forEach(numberOfRecords -> {
                Dataset<Row> requiredDs = inAvro.limit(numberOfRecords);
                String outputPath = outputDir + "/" + currentTimestamp + "/" + "records=" + numberOfRecords;
                System.out.println("{file_info: {Records: " + numberOfRecords + "," + "location:" + outputPath + "}}");
                requiredDs.write().mode("overwrite").format("avro").save(outputPath);
            });

            Dataset<Row> fileDetails = inAvro.select("private_jon_key", "location", "private_join_key_type").distinct();
            fileDetails.write().option("header", "true").mode("overwrite").csv(outputDir + "/processed_files/" + currentTimestamp);
        } catch (Exception exception){
            exception.printStackTrace();
            System.out.println("Mock Avro App failed with error : " + exception.getMessage());
            System.exit(1);
        }
    }
}

#! /bin/bash
spark-submit --verbose --class main.java.com.arismore.poste.spark.SparkJobsStarter --master local[*] --driver-memory 1g --executor-memory 1g --executor-cores 1 target/orestTAE-1.0-SNAPSHOT-jar-with-dependencies.jar 172.17.0.21:9092

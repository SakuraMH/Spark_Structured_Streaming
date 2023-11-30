package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.*;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.TimestampType;

public class Incidents {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        // Initialiser SparkSession
        SparkSession sparkS = SparkSession.builder()
                .appName("HospitalIncidentStreaming")
                .master("local[*]")
                .getOrCreate();

        // Définir le schéma des données
        StructType schema = new StructType()
                .add("Id", StringType, true)
                .add("titre", StringType, true)
                .add("description", StringType, true)
                .add("service", StringType, true)
                .add("date", StringType, true);

        // Lire les données en streaming
        Dataset<Row> streamingData = sparkS
                .readStream()
                .schema(schema)
                .csv("incidents");

        // Tâche 1 : Afficher le nombre d'incidents par service de manière continue
        StreamingQuery query1 = streamingData.groupBy("service")
                .count()
                .writeStream()
                .outputMode("complete")
                .format("console")
                .trigger(Trigger.ProcessingTime("1 minute"))
                .start();

        // Tâche 2 : Afficher les deux années avec le plus grand nombre d'incidents de manière continue
        StreamingQuery query2 = streamingData.groupBy(functions.year(functions.to_date(streamingData.col("date"))).alias("year"))
                .count()
                .orderBy(functions.desc("count"))
                .limit(2)
                .writeStream()
                .outputMode("complete")
                .format("console")
                .trigger(Trigger.ProcessingTime("10 seconds"))
                .start();

        // Attendre la fin du streaming
        query1.awaitTermination();
        query2.awaitTermination();


    }
}

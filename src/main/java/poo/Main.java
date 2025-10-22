package poo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) {
        SparkSession spark= SparkSession.builder()
                .appName("Incident SQL App")
                .master("local[*]")
                .config("spark.ui.port", "4080")
                .config("spark.port.maxRetries", "100")
                .getOrCreate();
        Dataset<Row> incidentDF= spark.read()
                .option("header","true")
                .option("inferSchema","true")
                .csv("incidents.csv");
        incidentDF.createOrReplaceTempView("incident");
        Dataset<Row> result = spark.sql("select service,count(*) from incident group by service");
        result.show();
        spark.stop();
        // 1. Afficher le nombre d’incidents par service.
        //2. Afficher les deux années où il a y avait plus d’incidents.
    }
}
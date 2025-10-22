package poo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) {
        SparkSession ss= SparkSession.builder()
                .appName("Spark SQL")
                .master("local[*]")
                .getOrCreate();
        Dataset<Row> df1= ss.read().csv("students.csv");
        df1.printSchema();

    }
}
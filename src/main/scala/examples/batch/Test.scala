package examples.batch

import org.apache.spark.sql.SparkSession

object Test extends App {

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("SQL Queries tester")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog")
    .config("spark.sql.catalog.spark_catalog.url", "jdbc:postgresql://localhost:5432/postgres?user=postgres&password=postgres")
    //.config("spark.sql.catalog.spark_catalog.dbtable", "test")
    .getOrCreate()

  spark.sql("CREATE TABLE pg1 USING jdbc OPTIONS ('url' 'jdbc:postgresql://localhost:5432/postgres?user=postgres&password=postgres', 'dbtable' 'pg1') AS SELECT 1 AS id, 'Crossdata' AS name, 'Stratio' AS company, 32 AS age").collect()
  spark.sql("CREATE TABLE pg2 USING jdbc OPTIONS ('url' 'jdbc:postgresql://localhost:5432/postgres?user=postgres&password=postgres', 'dbtable' 'pg2') AS SELECT 1 AS id, 'Crossdata' AS name, 'Stratio' AS company, 32 AS age").collect()
  spark.sql("SHOW DATABASES").show(500, false)
  spark.sql("SHOW TABLES IN public").show(500, false)
  spark.sql("SELECT * FROM (SELECT company, sum(age) age FROM public.pg1 GROUP BY company) t1, (SELECT company, sum(age) age FROM public.pg2 GROUP BY company) t2 WHERE t1.age >= t2.age").show(500, false)

  println("END")

  Thread.sleep(50000)

}
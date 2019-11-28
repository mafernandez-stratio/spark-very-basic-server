# spark-very-basic-server
Very basic server using Apache Spark to count vowels

## Instructions

- From project directory:

> mvn package

- For jar-with-dependencies

> mvn clean compile assembly:single

- From Spark home:

> bin/spark-submit --class examples.batch.VowelsCounter --master local[*] --deploy-mode client --executor-memory 1G verybasicserver/target/very-basic-server-0.1-SNAPSHOT.jar 9070

- For jar execution

> java -jar target/very-basic-server-0.1-SNAPSHOT-jar-with-dependencies.jar "9070 spark://localhost:7077"

- From a terminal:

> nc localhost 9070

- From a web browser:

> http://localhost:4040/jobs/

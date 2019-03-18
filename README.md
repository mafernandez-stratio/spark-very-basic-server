# spark-very-basic-server
Very basic server using Apache Spark to count vowels

## Instructions

- From project directory:

> mvn package

- From Spark home:

> bin/spark-submit --class utad.VowelsCounter --master local[*] --deploy-mode client --executor-memory 1G verybasicserver/target/very-basic-server-0.1-SNAPSHOT.jar 9070

- From a terminal:

> nc localhost 9070

- From a web browser:

> http://localhost:4040/jobs/

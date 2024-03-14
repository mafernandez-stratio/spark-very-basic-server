# spark-very-basic-server
Very basic server using Apache Spark to count vowels

## Instructions

- From project directory:

> mvn package

- For jar-with-dependencies

> mvn clean compile assembly:single

- From Spark home:

> bin/spark-submit --class examples.batch.VowelsCounter --master local[*] --deploy-mode client --executor-memory 1G verybasicserver/target/very-basic-server-0.1-SNAPSHOT.jar 9070

> bin/spark-submit --class examples.streaming.TwitterSentimentAnalysis --master local[*] --deploy-mode client --executor-memory 1G --jars /Users/miguelangelfernandezdiaz/Downloads/postgresql-42.2.24.jar --driver-class-path /Users/miguelangelfernandezdiaz/Downloads/postgresql-42.2.24.jar /Users/miguelangelfernandezdiaz/workspace/verybasicserver/target/very-basic-server-0.1-SNAPSHOT-jar-with-dependencies.jar /root/twitter.properties jdbc:postgresql://localhost:5432/postgres?user=root\&postgres=root

- For jar execution

> java -jar target/very-basic-server-0.1-SNAPSHOT-jar-with-dependencies.jar "9070" "spark://localhost:7077"

- From a terminal:

> nc localhost 9070

- From a web browser:

> http://localhost:4040/jobs/

# /home/heirish/maven/bin/mvn clean package  -Dmaven.test.skip=true
set -x
/home/heirish/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.1 \
--class com.company.platform.team.projspark.StructuredStream  \
--master local \
--conf spark.driver.extraJavaOptions=-Dlog4j.debug \
--driver-class-path target/logmine-demo-1.0-SNAPSHOT.jar \
target/logmine-demo-1.0-SNAPSHOT.jar -b 35.202.221.169:9092 -t nelo2-normal-logs > spark.log
#--conf spark.driver.extraJavaOptions=-Dlog4j.configuration=src/main/java/resources/log4j.xml \
#--conf spark.executor.extraJavaOptions=-Dlog4j.configuration=src/main/java/resources/log4j.xml \

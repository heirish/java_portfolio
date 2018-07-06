/home/heirish/apps/maven/bin/mvn clean package  -Dmaven.test.skip=true

tasktype=$1

set -x
jarpackage=target/structued-stream-1.0-SNAPSHOT.jar

if [[ $tasktype == spark ]]; then
    rm -rf spark-warehouse
    rm -rf output*
    rm -rf patternbase*
    /home/heirish/apps/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.1 \
    --verbose \
    --class com.company.platform.team.projspark.StructuredStream  \
    --master local \
    --conf spark.driver.extraJavaOptions=-Dlog4j.debug \
    --driver-class-path ${jarpackage} \
    ${jarpackage} -j spark -b localhost:9092 -t test> spark.log
else 
    rm -rf patternoutput-*
    /home/heirish/apps/hadoop/bin/hadoop jar ${jarpackage} com.company.platform.team.projspark.StructuredStream \
    -j hadoop -i patternbase -r "(.*.json)|(.*/patternoutput-.*/part-.*)" \
    -o patternoutput > hadoop.log
fi

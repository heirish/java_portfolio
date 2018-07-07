/home/heirish/apps/maven/bin/mvn clean package  -Dmaven.test.skip=true

tasktype=$1

set -x
jarpackage=target/structued-stream-1.0-SNAPSHOT.jar
mainclass=com.company.platform.team.projspark.PatternRecognize

if [[ $tasktype == spark ]]; then
    rm -rf spark-warehouse
    rm -rf output*
    rm -rf patternbase*
    /home/heirish/apps/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.1 \
    --verbose \
    --class ${mainclass} \
    --master local \
    --conf spark.driver.extraJavaOptions=-Dlog4j.debug \
    --driver-class-path ${jarpackage} \
    ${jarpackage} -j spark -c cursoryfinder.json > spark.log
else 
    rm -rf patternoutput-*
    /home/heirish/apps/hadoop/bin/hadoop jar ${jarpackage} ${mainclass} \
    -j hadoop -c patternrefiner.json > hadoop.log
fi

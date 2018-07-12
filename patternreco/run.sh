#/home/heirish/apps/maven/bin/mvn clean package  -Dmaven.test.skip=true

tasktype=$1

set -x
jarpackage=target/patternreco-1.0-SNAPSHOT.jar
mainclass=com.company.platform.team.projpatternreco.sparkandhadoop.PatternRecognize

if [[ $tasktype == spark ]]; then
    rm -rf spark-warehouse
    rm -rf dataout/logout*
    rm -rf dataout/logpatternout*
    /home/heirish/apps/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.1 \
    --verbose \
    --class ${mainclass} \
    --master local \
    --conf spark.driver.extraJavaOptions=-Dlog4j.debug \
    --driver-class-path ${jarpackage} \
    ${jarpackage} -j spark -c conf/cursoryfinder.json > log/spark.log
else 
    rm -rf patternoutput-*
    /home/heirish/apps/hadoop/bin/hadoop jar ${jarpackage} ${mainclass} \
    -j hadoop -c conf/patternrefiner.json > log/hadoop.log
fi

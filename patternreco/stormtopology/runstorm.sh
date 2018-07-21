#/home/heirish/apps/maven/bin/mvn clean package -Dmaven.test.skip=true

set -x
jarpackage=target/stormtopology-1.0-SNAPSHOT.jar
mainclass=com.company.platform.team.projpatternreco.stormtopology.PatternRecognizeTopology

nohup storm jar ${jarpackage} ${mainclass} > storm.log &

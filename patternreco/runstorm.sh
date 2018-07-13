/home/heirish/apps/maven/bin/mvn clean package  -Dmaven.test.skip=true

set -x
jarpackage=target/patternreco-1.0-SNAPSHOT.jar
mainclass=com.company.platform.team.projpatternreco.stormtopology.PatternRecognizeTopology

storm jar ${jarpackage} ${mainclass}

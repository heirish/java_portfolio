#/home/heirish/apps/maven/bin/mvn clean test -Dtest=PatternNodeServerTest#startServerTest
/home/heirish/apps/maven/bin/mvn clean package  -Dmaven.test.skip=true
java -cp target/patternreco-1.0-SNAPSHOT.jar  com.company.platform.team.projpatternreco.stormtopology.leaffinder.ComputeCPULoadTest -n 10

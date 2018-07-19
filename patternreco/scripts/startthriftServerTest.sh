#/home/heirish/apps/maven/bin/mvn clean test -Dtest=PatternNodeServerTest#startServerTest
/home/heirish/apps/maven/bin/mvn clean package  -pl sparkhadoopJob -Dmaven.test.skip=true
java -cp target/sparkhadoopJob-1.0-SNAPSHOT.jar  com.company.platform.team.projpatternreco.sparkandhadoop.ThriftServerShell

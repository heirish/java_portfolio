#/home/heirish/apps/maven/bin/mvn clean test -Dtest=com.company.platform.team.projspark.PatternNodeHelper.PatternNodeServerTest#startServerTest
/home/heirish/apps/maven/bin/mvn clean package  -Dmaven.test.skip=true
java -cp target/structued-stream-1.0-SNAPSHOT.jar com.company.platform.team.projspark.ThriftServerShell

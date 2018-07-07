namespace java com.company.platform.team.projspark.PatternNodeHelper

typedef i32 int //typedefs to get convenient names for your types  
typedef i16 short
typedef i64 long

service PatternCenterThriftService {
    string getNodes(1:string projectName, 2:int nodeLevel)
    string synchronizeNodes(1:string projectName, 2:int nodeLevel, 3:long latestUpdatedTime)
    string addNode(1:string nodeInfo, 2:long latestUpdatedTime)
    string updateNode(1:string nodeInfo)
}

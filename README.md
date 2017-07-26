# Cassandra-Auto-Repair

使用方法：

将脚本放在cassandra的bin目录下，运行python autoRepair.py keyspaceInfo.txt进行自动修复

上一过程正常或非正常终止都会在目录留下txt文件，下次需要继续修复可执行python xxxRepair.py ip filename.txt [storeInterval]

其中，ip应与体现在filename中的ip对应；storeInterval为可选参数，用来控制向filename.txt写入修复记录的间隔，默认为1


keyspaceInfo.txt格式：
replicaNum1 keyspace1 keyspace2
replicaNum2 keyspace3 keyspace4 keyspace5
replicaNum3 keyspace6
replicaNum4 keyspace7 keyspace8 ...

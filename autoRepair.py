# -*- coding: utf-8 -*-

import os
import re
import sys

class virtualNode:
    def __init__(self, ip, token):
        self.ip = ip
        self.token = token

def getTokenInText(text):
    return re.findall(r"(-?\d+)", text)[-1]

def getIpInText(text):
    return re.findall(r"(\d+\.\d+\.\d+\.\d+)", text)[0]

# 获取所有存活节点的ip地址，并存入字典ipToRange的键中
# 同时获取所有死亡节点的ip地址，并存入集合deadNode中
text = os.popen("./nodetool status").readlines()
ipToRange = dict()
deadNode = set()
for i in range(5, len(text) - 1):
    if "DN" in text[i]:
        deadNode.add(getIpInText(text[i]))
    else:
        ipToRange[getIpInText(text[i])] = set()

# 若没有节点死亡，则无需进行之后的任何操作
if len(deadNode) == 0:
    print "All nodes are alive"
    sys.exit(0)

text = os.popen("./nodetool ring").readlines()
needToRepairOri = set()
virtualNodes = []

# 将所有的ip与token的对应关系存入virtualNodes中
# 将初步获得的需要修复的range编号存入needToRepairOri中
# 将存活节点对应的primary range存入ipToRange的值中
print "Process 1 : Storing all virtual nodes and original ranges that need to be repaired"
for i in range(5, len(text) - 6):
    ip = getIpInText(text[i])
    virtualNodes.append(virtualNode(ip, getTokenInText(text[i])))
    if 'Down' in text[i]:
        needToRepairOri.add(i - 5)
    else:
        ipToRange[ip].add(i - 5)
print "Process 1 finished"

# 死掉的节点对应的range会并入它的下一个存活节点中
# 对ipToRange的值进行上述扩充
for i in needToRepairOri:
    j = i + 1 if i + 1 < len(virtualNodes) else 0
    while virtualNodes[j].ip in deadNode:
        j = j + 1 if j + 1 < len(virtualNodes) else 0
    ipToRange[virtualNodes[j].ip].add(i)

print "Process 2 : Searching for all ranges that need to be repaired"
keyspaceInfo = open(sys.argv[1]).readlines()
for info in keyspaceInfo:
    # 删去行尾的换行符
    info = info.replace('\n', '')
    info = info.replace('\r', '')
    splitInfo = info.split(" ")
    numOfReplicas = splitInfo[0]
    # 副本数小于2则无需修复
    if numOfReplicas < 2:
        continue
    needToRepairAll = set()
    # 通过needToRepairOri算出所有需要修复的range编号
    # 对于一个已知的需要修复的range i，将它的对应ip1存入replicaIp中
    # 寻找它前一个range对应的ip2，当ip2等于ip1时，终止寻找
    # 否则，只要ip2在replicaIp中或replicaIp的长度小于副本数，将ip2加入集合replicaIp中，并将range编号存入needToRepairAll中
    # 若不满足前一条，同样终止寻找
    for i in needToRepairOri:
        ip1 = virtualNodes[i].ip
        replicaIp = {ip1}
        j = len(virtualNodes) - 1 if i - 1 < 0 else i - 1
        while j != i:
            ip2 = virtualNodes[j].ip
            if ip2 == ip1:
                break
            elif ip2 in replicaIp or len(replicaIp) < numOfReplicas:
                replicaIp.add(ip2)
                needToRepairAll.add(j)
            else:
                break
            j = len(virtualNodes) - 1 if j - 1 < 0 else j - 1
        if i == j:
            print "Warn: The number of nodes is less than the num of replicas!"
    needToRepairAll = needToRepairAll | needToRepairOri

    keyspaces = splitInfo[1:]
    # 通过ipToRange与needToRepairAll取交集获得每个节点需要修复的primary range
    # 将各个节点需要修复的range写入文件储存
    for keyspace in keyspaces:
        for ip in ipToRange:
            fp = open(keyspace + "_" + ip.replace(".", "_") + ".txt", 'w')
            fp.writelines(keyspace + "\n")
            tempRangeSet = ipToRange[ip] & needToRepairAll
            numOfRanges = len(tempRangeSet)
            for i in tempRangeSet:
                j = len(virtualNodes) - 1 if i - 1 < 0 else i - 1
                fp.write(virtualNodes[j].token + " " + virtualNodes[i].token + "\n")
            fp.writelines('0 ranges have been repaired' + '\n')
            fp.close()
print "Process 2 finished"

# 暗杀死亡的节点
print "Process 3 : Assassinating dead nodes"
for i in deadNode:
    print "Process 3 : Assassinating " + i
    os.system("./nodetool assassinate %s" % i)
print "Process 3 finished"

# 调用xxxRepair进行修复
print "autoRepair : Starting xxxRepair"

if len(sys.argv) < 3:
    sys.argv = ["xxxRepair.py", "", ""]
else:
    sys.argv = ["xxxRepair.py", "", "", sys.argv[2]]

for info in keyspaceInfo:
    # 删去行尾的换行符
    info = info.replace('\n', '')
    info = info.replace('\r', '')
    splitInfo = info.split(" ")
    if int(splitInfo[0]) < 2:
        continue
    for keyspace in splitInfo[1:]:
        for ip in ipToRange:
            sys.argv[1] = ip
            sys.argv[2] = keyspace + "_" + ip.replace(".", "_") + ".txt"
            execfile("xxxRepair.py")
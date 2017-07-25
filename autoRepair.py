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

# Get all ip address
text = os.popen("nodetool status").readlines()
ipToRange = dict()
for i in range(5, len(text) - 1):
    if "DN" in text[i]:
        continue
    ipToRange[getIpInText(text[i])] = set()

text = os.popen("nodetool ring").readlines()
keyspaceAndCF = sys.argv[1]
numOfReplicas = sys.argv[2]
needToRepairOri = set()
needToRepairAll = set()
localRange = set()
deadNode = set()
virtualNodes = []

# Store all virtual nodes and original ranges that need to be repaired
print "Process 1 : Storing all virtual nodes and original ranges that need to be repaired"
for i in range(5, len(text) - 6):
    ip = getIpInText(text[i])
    virtualNodes.append(virtualNode(ip, getTokenInText(text[i])))
    if 'Down' in text[i]:
        deadNode.add(ip)
        needToRepairOri.add(i - 5)
    else:
        ipToRange[ip].add(i - 5)
print "Process 1 finished"

if len(needToRepairOri) == 0:
    print "All nodes are alive"
    sys.exit(0)

for i in needToRepairOri:
    j = i + 1 if i + 1 < len(virtualNodes) else 0
    while virtualNodes[j].ip in deadNode:
        j = j + 1 if j + 1 < len(virtualNodes) else 0
    ipToRange[virtualNodes[j].ip].add(i)

# Find all ranges that need to be repaired
print "Process 2 : Searching for all ranges that need to be repaired"
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
print "Process 2 finished"

for ip in ipToRange:
    fp = open(keyspaceAndCF + "_" + ip.replace(".", "_") + ".txt", 'w')
    fp.writelines(keyspaceAndCF + "\n")
    ipToRange[ip] = ipToRange[ip] & needToRepairAll
    numOfRanges = len(ipToRange[ip])
    for i in ipToRange[ip]:
        j = len(virtualNodes) - 1 if i - 1 < 0 else i - 1
        fp.write(virtualNodes[j].token + " " + virtualNodes[i].token + "\n")
    fp.writelines('0 ranges have been repaired' + '\n')
    fp.close()

# Assassinate dead nodes
print "Process 3 : Assassinating dead nodes"
for i in deadNode:
    print "Process 3 : Assassinating " + i
    os.system("nodetool assassinate %s" % i)
print "Process 3 finished"

print "autoRepair : Starting xxxRepair"
for ip in ipToRange:
    sys.argv = ["xxxRepair.py", ip, keyspaceAndCF + "_" + ip.replace(".", "_") + ".txt"]
    execfile("xxxRepair.py")
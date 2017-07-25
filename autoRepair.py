import os
import re
import sys
import threading

class virtualNode:
    def __init__(self, ip, token):
        self.ip = ip
        self.token = token

def getTokenInText(text):
    return re.findall(r" (-?\d+)", text)[-1]

def getIpInText(text):
    return re.findall(r"(\d+\.\d+\.\d+\.\d+)", text)[0]

text = os.popen("./nodetool ring").readlines()
# text = open("tokens").readlines()
keyspaceAndCF = sys.argv[1]
numOfReplicas = sys.argv[2]
needToRepairOri = set()
needToRepairAll = set()
deadNode = set()
virtualNodes = []

fp = open(keyspaceAndCF + ".txt", 'w')
fp.writelines(keyspaceAndCF + "\n")

# Store all virtual nodes and original ranges that need to be repaired
print "Process 1 : Storing all virtual nodes and original ranges that need to be repaired"
for i in range(5, len(text) - 6):
    virtualNodes.append(virtualNode(getIpInText(text[i]), getTokenInText(text[i])))
    if 'Down' in text[i]:
        deadNode.add(getIpInText(text[i]))
        needToRepairOri.add(i - 5)
print "Process 1 finished"

if len(needToRepairOri) == 0:
    print "All nodes are alive"
    sys.exit(0)

# Find all ranges that need to be repaired
print "Process 2 : Searching for all ranges that need to be repaired"
for i in needToRepairOri:
    ip1 = virtualNodes[i].ip
    replicaDict = {ip1 : i}
    j = len(virtualNodes) - 1 if i - 1 < 0 else i - 1
    while j != i and len(replicaDict) < numOfReplicas:
        ip2 = virtualNodes[j].ip
        if ip2 == ip1:
            break
        else:
            replicaDict[ip2] = j
        j = len(virtualNodes) - 1 if j - 1 < 0 else j - 1
    if i == j:
        print "Warn: The number of nodes is less than the num of replicas!"
    for value in replicaDict.values():
        needToRepairAll.add(value)
needToRepairAll = needToRepairAll | needToRepairOri
print "Process 2 finished"

numOfRanges = len(needToRepairAll)
for i in needToRepairAll:
    j = len(virtualNodes) - 1 if i - 1 < 0 else i - 1
    fp.write(virtualNodes[j].token + " " + virtualNodes[i].token + "\n")

# Assassinate dead nodes
print "Process 3 : Assassinating dead nodes"
for i in deadNode:
    print "Process 3 : Assassinating " + i
    os.system("./nodetool assassinate %s" % i)
print "Process 3 finished"

terminateProcess = False
def terminateProcessManually():
    print raw_input()
    global terminateProcess
    terminateProcess = True
terminateThread = threading.Thread(target=terminateProcessManually)
terminateThread.start()

print "Process 4: Repairing all ranges that need to be repaired"
hasFailure = False
numOfRepaired = 0
fpFailed = open(keyspaceAndCF + "_failed.txt", "w")
fpFailed.write(keyspaceAndCF + '\n')
# Repair all ranges that need to be repaired
for i in needToRepairAll:
    if terminateProcess == True:
        print "Process 4 is terminated manually"
        break
    numOfRepaired = numOfRepaired + 1
    j = len(virtualNodes) - 1 if i - 1 < 0 else i - 1
    log = os.popen("./nodetool repair -st %s -et %s %s" %\
          (virtualNodes[j].token, virtualNodes[i].token, keyspaceAndCF)).read()
    if "successfully" in log:
        print str(numOfRepaired) + " / " + str(numOfRanges) + " has been repaired successfully"
    else:
        hasFailure = True
        print "Failed to repair range " + str(numOfRepaired)
        print log
        fpFailed.write(virtualNodes[j].token + " " + virtualNodes[i].token + "\n")
    print "Press Enter to terminate the process"
print "Process 4 finished"

if hasFailure:
    print str(numOfRepaired) + ' ranges have been repaired with some failures'
    fpFailed.writelines(0 + ' ranges have been repaired' + '\n')
    fpFailed.close()
else:
    print str(numOfRepaired) + ' ranges have been repaired'
    fpFailed.close()
    os.remove(keyspaceAndCF + "_failed.txt")
fp.writelines(str(numOfRepaired) + ' ranges have been repaired' + '\n')
fp.close()

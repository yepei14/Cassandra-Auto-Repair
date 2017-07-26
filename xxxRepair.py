# -*- coding: utf-8 -*-
import os
import re
import sys
import threading

def getNumbersInText(text):
    return re.findall(r"(-?\d+)", text)

# 获取需修复的键空间名称keyspaceAndCF，总的需修复条数totalRangeNum 和已修复的条数等数据repairedNum
fp = open(sys.argv[2], 'r')
ip = sys.argv[1]
text = fp.read()
fp.close()
logNum = text.count('ranges have been repaired')
text = text.split('\n')
totalRangeNum = len(text) - logNum - 2
keyspaceAndCF = text[0]
repairedNum = int(getNumbersInText(text[-2])[0]) if logNum != 0 else 0

# 若totalRangeNum与repairedNum相等，则所有修复已完成
if repairedNum == totalRangeNum:
    print "All ranges have been repaired!"
    sys.exit(0)

print "xxxRepair : Repairing keyspace " + keyspaceAndCF + " on node " + sys.argv[1]
hasFailure = False
fp = open(sys.argv[2], 'a')
fpFailed = open(sys.argv[2][:-4] + "_failed.txt", "w")
fpFailed.write(keyspaceAndCF + '\n')
fpFailed.flush()

# 手动停止修复
# terminateThread一直因等待输入而阻塞
# 获得输入后，将terminateProcess置为True，从而终止主进程
# 同时将需要存储的信息存入文件
terminateProcess = False
def terminateProcessManually():
    print raw_input()
    global terminateProcess
    terminateProcess = True
terminateThread = threading.Thread(target=terminateProcessManually)
terminateThread.start()

# 逐条修复需要修复的ranges
# storeInterval为存储修复进程的间隔
storeInterval = int(sys.argv[3]) if len(sys.argv) == 4 else 1
for i in range(repairedNum + 1, totalRangeNum + 1):
    if terminateProcess == True:
        print "xxxRepair is terminated manually"
        break
    print "Repairing range " + str(repairedNum + 1) + " ..."
    log = os.popen("./nodetool -h %s repair -st %s -et %s %s" % (ip,\
            getNumbersInText(text[i])[0], getNumbersInText(text[i])[1], keyspaceAndCF)).read()
    repairedNum = repairedNum + 1
    if "successfully" in log:
        print str(repairedNum) + " / " + str(totalRangeNum) + " has been repaired successfully"
        if repairedNum % storeInterval == 0:
            fp.writelines(str(repairedNum) + ' ranges have been repaired' + '\n')
            fp.flush()
    else:
        hasFailure = True
        print "Failed to repair range " + str(repairedNum) + " / " + str(totalRangeNum)
        print log
        if fpFailed.closed != True:
            fpFailed.write(getNumbersInText(text[i])[0] + " " + getNumbersInText(text[i])[1] + "\n")
            fp.flush()
    print "Press Enter to terminate the process"

if hasFailure:
    print str(repairedNum) + ' ranges have been repaired with some failures'
    fpFailed.writelines('0 ranges have been repaired' + '\n')
    fpFailed.close()
else:
    print str(repairedNum) + ' ranges have been repaired'
    fpFailed.close()
    os.remove(sys.argv[2][:-4] + "_failed.txt")
fp.writelines(str(repairedNum) + ' ranges have been repaired' + '\n')
fp.close()
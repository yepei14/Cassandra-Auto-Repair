import os
import re
import sys
import threading

def getNumbersInText(text):
    return re.findall(r"(-?\d+)", text)

fp = open(sys.argv[1], 'r')
text = fp.read()
fp.close()
logNum = text.count('ranges have been repaired')
text = text.split('\n')
totalRangeNum = len(text) - logNum - 2
keyspaceAndCF = text[0]
repairedNum = int(getNumbersInText(text[-2])[0])

terminateProcess = False
def terminateProcessManually():
    print raw_input()
    print "Repair is terminated manually"
    global terminateProcess
    terminateProcess = True
terminateThread = threading.Thread(target=terminateProcessManually)
terminateThread.start()

hasFailure = False
fpFailed = open(sys.argv[1][:-4] + "_failed.txt", "w")
fpFailed.write(keyspaceAndCF + '\n')
# Repair all ranges that need to be repaired
for i in range(repairedNum + 1, totalRangeNum + 1):
    if terminateProcess == True:
        break
    repairedNum = repairedNum + 1
    log = os.popen("./nodetool repair -st %s -et %s %s" % \
                   (getNumbersInText(text[i])[0], getNumbersInText(text[i])[1], keyspaceAndCF)).read()
    if "successfully" in log:
        print str(repairedNum) + " / " + str(totalRangeNum) + " has been repaired successfully"
    else:
        hasFailure = True
        print "Failed to repair range " + str(repairedNum)
        print log
        fpFailed.write(getNumbersInText(text[i])[0] + " " + getNumbersInText(text[i])[1] + "\n")
    print "Press Enter to terminate the process"

if hasFailure:
    print str(repairedNum) + ' ranges have been repaired with some failures'
    fpFailed.writelines(0 + ' ranges have been repaired' + '\n')
    fpFailed.close()
else:
    print str(repairedNum) + ' ranges have been repaired'
    fpFailed.close()
    os.remove(sys.argv[1][:-4] + "_failed.txt")
fp = open(sys.argv[1], 'a')
fp.writelines(str(repairedNum) + ' ranges have been repaired' + '\n')
fp.close()

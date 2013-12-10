__author__ = 'Oliver Roess'

import re
from cStringIO import StringIO

def parseStderr(stderr):
    """Parse output and return CSV StringIO object.
    :param stderr: a file object containing the stderr of ycsb (using -s)
    """

    csvString = StringIO()

    # write header
    print >>csvString, "Time (s),Operations,Throughput (ops/s),Insert(ms),Update(ms),Read(ms)"

    time = []
    ops = []
    tp = []
    update = []
    read = []
    insert = []

    length = 0

    while True:
        line = stderr.readline()

        if line == '':
            break

        line = line.strip()

        if not re.match(r"\d", line):
            continue
        if re.match(r"0", line):
            continue

        # line = re.sub
        data = re.split(r" sec: ", line)
        time.append(data[0])
        data = re.split(r"; ", data[1])
        # data[0] now contains "2222 operations"
        # data[1] now contains "222.22 current ops/sec"
        # data[2] now contains the rest (e.g. [UPDATE AverageLatency....] [READ....])
        ops.append(re.sub(" operations", "", data[0]))
        tp.append(re.sub("\s.*", "", data[1]))
        try:
            data = re.split("\] \[", data[2])
            for metric in data:
                metric = re.sub(r"(\[|\])", "", metric)
                temp = re.split("=", metric)
                res = float(temp[1])/1000
                if re.match(r"U", metric):
                    update.append(res)
                elif re.match(r"I", metric):
                    insert.append(res)
                elif re.match(r"R", metric):
                    read.append(res)
        except:
            print("Parsing problem for stderr")
            pass


    for i in range(len(time)):
        csvString.write(time[i]+","+ops[i]+","+tp[i]+",")
        if i < len(insert):
            csvString.write(str(insert[i]))
        csvString.write(",")
        if i < len(update):
            csvString.write(str(update[i]))
        csvString.write(",")
        if i < len(read):
            csvString.write(str(read[i]))
        csvString.write("\n")

    return csvString


def parseTimeseries(stdout):
    """Parse output and return CSV StringIO object.
    :param stdout: a file object containing the stdout of ycsb timeseries mode

    ATTENTION: modified for personal modified YCSB output
    """

    csvString = StringIO()
    print >>csvString, "Time (s),INSERT (ms),UPDATE (ms),READ (ms)"

    time = [0] # integer
    insert = []
    update = []
    read = []

    while True:
        line = stdout.readline()

        if line == '':
            break

        if not re.match(r"\[ts_\w+\], \d+,", line):
            continue

        line = line.strip()

        if re.match(r"\[ts_I", line):
            line = re.sub(r"\[ts_\w+\], ", "", line)
            split = re.split(r", ", line)
            insert.append(float(split[1])/1000)

            if max(time) < int(split[0])/1000:
                time.append(int(split[0])/1000)
        elif re.match(r"\[ts_U", line):
            line = re.sub(r"\[ts_\w+\], ", "", line)
            split = re.split(r", ", line)
            update.append(float(split[1])/1000)

            if max(time) < int(split[0])/1000:
                time.append(int(split[0])/1000)
        elif re.match(r"\[ts_READ\]", line):
            line = re.sub(r"\[ts_\w+\], ", "", line)
            split = re.split(r", ", line)
            read.append(float(split[1])/1000)

            if max(time) < int(split[0])/1000:
                time.append(int(split[0])/1000)

    for i in range(len(time)):
        csvString.write(str(time[i])+",")
        if i < len(insert):
            csvString.write(str(insert[i]))
        csvString.write(",")
        if i < len(update):
            csvString.write(str(update[i]))
        csvString.write(",")
        if i < len(read):
            csvString.write(str(read[i]))
        csvString.write("\n")

    return csvString


def parseHistogram(stdout):
    """Parse output and return CSV StringIO object.
    :param stdout: a file object containing the stdout of ycsb histogram mode (default mode)
    """
    csvString = StringIO()

    # write header
    print >>csvString, "ms,INSERT,Prob,CumProb,UPDATE,Prob,CumProb,READ,Prob,CumProb"

    insert = []
    update = []
    read = []

    length = 0

    while True:
        line = stdout.readline()

        if line == '':
            break

        if not re.match(r"\[\w+\], >?\d+,", line):
            continue

        line = line.strip()

        if re.match(r"\[I", line):
            line = re.sub(r"\[\w+\], >?\d+, ", "", line)
            insert.append(int(line))
            length = len(insert)
        elif re.match(r"\[U", line):
            line = re.sub(r"\[\w+\], >?\d+, ", "", line)
            update.append(int(line))
            length = len(update)
        elif re.match(r"\[READ\]", line):
            line = re.sub(r"\[\w+\], >?\d+, ", "", line)
            read.append(int(line))
            length = len(read)


    # do some extra calculations
    sumInsert = sum(insert)
    sumUpdate = sum(update)
    sumRead = sum(read)

    densityInsert = []
    densityUpdate = []
    densityRead = []

    for i in range(length):
        densityInsert.append(float(insert[i])/float(sumInsert)) if insert else None
        densityUpdate.append(float(update[i])/float(sumUpdate)) if update else None
        densityRead.append(float(read[i])/float(sumRead)) if read else None

    distInsert = []
    distUpdate = []
    distRead = []

    distInsert.append(densityInsert[0]) if densityInsert else None
    distUpdate.append(densityUpdate[0]) if densityUpdate else None
    distRead.append(densityRead[0]) if densityRead else None


    # write csv
    for i in range(length):

        csvString.write(str(i)+",")
        if insert:
            distInsert.append(distInsert[i-1] + densityInsert[i]) if i > 0 else None
            csvString.write(str(insert[i])+","+str(densityInsert[i])+","+str(distInsert[i])+",")
        else:
            csvString.write(",,,")
        if update:
            distUpdate.append(distUpdate[i-1] + densityUpdate[i]) if i > 0 else None
            csvString.write(str(update[i])+","+str(densityUpdate[i])+","+str(distUpdate[i])+",")
        else:
            csvString.write(",,,")
        if read:
            distRead.append(distRead[i-1] + densityRead[i]) if i > 0 else None
            csvString.write(str(read[i])+","+str(densityRead[i])+","+str(distRead[i])+",")

        csvString.write("\n")

    return csvString


# f = open("/home/xubuntu/Desktop/stdout.txt", "r")
# csvString = parseTimeseries(f)
# f.close()
#
# print csvString.getvalue()

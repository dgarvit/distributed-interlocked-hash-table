with open("time.txt") as f:
    content = f.readlines()

times = []
for c in content:
    split = c.split()
    if len(split) == 2:
        split[0] = float(split[0])
        times.append(split)

times.sort(key=lambda tup: (tup[1], tup[0]), reverse=True)

maxStr = "Max " + str(times[0][1]) + ": " + str(times[0][0])
print maxStr

for i in xrange(0, len(times)-1):
    if str(times[i][1]) != str(times[i+1][1]):
        minStr = "Min " + str(times[i][1]) + ": " + str(times[i][0])
        maxStr = "Max " + str(times[i+1][1]) + ": " + str(times[i+1][0])
        print minStr
        print maxStr

minStr = "Min " + str(times[-1][1]) + ": " + str(times[-1][0])
print minStr

with open("sortByFunctions.txt", 'w') as f:
    for a in times:
        f.write("%s\n" % a)

times.sort(key=lambda tup: tup[0], reverse=True)

with open("sortByTime.txt", 'w') as f:
    for a in times:
        f.write("%s\n" % a)

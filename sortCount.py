with open("count.txt") as f:
    content = f.readlines()

counts = []

for c in content:
    split = c.split()
    if len(split) == 3:
        for i in xrange(0, len(split)):
            split[i] = int(split[i])
        counts.append(split)

counts.sort(key=lambda tup: tup[0], reverse=True)

print counts[0]

with open("sortCount.txt", 'w') as f:
    for a in counts:
        f.write("%s\n" % a)

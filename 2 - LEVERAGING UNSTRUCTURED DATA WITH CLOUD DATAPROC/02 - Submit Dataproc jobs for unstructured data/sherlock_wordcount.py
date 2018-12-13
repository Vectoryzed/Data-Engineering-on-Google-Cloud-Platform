lines = sc.textFile("/sampledata/sherlock-holmes.txt")
type(lines)
lines.count()
lines.take(15)

words =  lines.flatMap(lambda x: x.split(' '))
type(words)
words.count()
words.take(15)

pairs = words.map(lambda x: (x,len(x)))
type(pairs)
pairs.count()
pairs.take(5)

pairs = words.map(lambda x: (len(x),1))
pairs.take(5)

from operator import add
wordsize = pairs.reduceByKey(add)
type(wordsize)
wordsize.count()
wordsize.take(5)

output = wordsize.collect() # not sorted
type(output)
for (size,count) in output: print(size, count)

output = wordsize.sortByKey().collect()
for (size,count) in output: print(size, count)


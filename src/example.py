from pyspark import SparkContext, SparkConf
import random

class MyClass:
    def myFunc(self, s):
        words = s.split(" ")
        return len(words)

    def doStuff(self, rdd):
        myFunc = self.myFunc
        return rdd.map(myFunc)

    def inside(self, p):
        x, y = random.random(), random.random()
        return x * x + y * y < 1

    def estimatePI(self, rdd):
        inside = self.inside
        return rdd.filter(inside).count()



if __name__ == "__main__":

    #count words
    myClass = MyClass()
    conf = SparkConf().setAppName("wordCount").setMaster("local[4]")
    sc = SparkContext(conf=conf)
    print("Done configure")
    lines = sc.textFile(name="../data/a_text_file")
    lineLengths = myClass.doStuff(lines)
    totalLength = lineLengths.reduce(lambda a, b: a + b)

    print(totalLength)


    # estimate PI
    NUM_SAMPLES = 100000000
    temp_data = sc.parallelize(xrange(0, NUM_SAMPLES))
    esPI = myClass.estimatePI(temp_data)

    print "PI is estimated as ", esPI * 4.0 / NUM_SAMPLES



'''

conf = SparkConf().setAppName("wordCount").setMaster("local[4]")
sc = SparkContext(conf=conf)
print("Done configure")
# text_file = sc.textFile(name="../data/a _text_file")
lines = sc.textFile(name="../data/a_text_file")
lineLengths = lines.map(lambda line: len(line))
totalLength = lineLengths.reduce(lambda a, b: a + b)

print(totalLength)


# counts = text_file.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
# counts.saveAsTextFile("../data/count_of_01_text_file")
'''

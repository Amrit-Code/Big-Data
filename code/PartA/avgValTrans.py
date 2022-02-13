from mrjob.job import MRJob
import time
class CWPartA2(MRJob):

    def mapper(self, _, line):
        try:
            fields = line.split(",")
            if len(fields) == 7:
                timeStamp = int(fields[6])
                date = time.strftime("%m - %Y",time.gmtime(timeStamp))
                yield (date,(int(fields[3]),1))
        except:
            pass

    def combiner(self, word, counts):
        count = 0
        total = 0
        for i in counts:
            count += i[1]
            total += i[0]
        yield(word, (total, count))


    def reducer(self, word, counts):
        count = 0
        total = 0
        for i in counts:
            count += i[1]
            total += i[0]
        yield(word, (total/count))
            


if __name__ == '__main__':
    CWPartA2.run()

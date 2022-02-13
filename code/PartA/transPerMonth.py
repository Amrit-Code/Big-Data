from mrjob.job import MRJob
import time
class CWPartA1(MRJob):

    def mapper(self, _, line):
        try:
            fields = line.split(",")
            if len(fields) == 7:
                timeStamp = int(fields[6])
                date = time.strftime("%m - %Y",time.gmtime(timeStamp))
                yield (date,1)
        except:
            pass
    
    def combiner(self, word, counts):
        yield(word,sum(counts))

    def reducer(self, word, counts):
        yield(word,sum(counts))
            


if __name__ == '__main__':
    CWPartA1.run()

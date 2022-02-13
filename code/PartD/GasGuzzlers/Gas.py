from mrjob.job import MRJob
import time

class Gas(MRJob):

    def mapper(self, _, line):
        try:
            fields = line.split(",")
            if len(fields) == 7:
                tTime = int(fields[6])
                price = int(fields[5])
                month = time.strftime("%B %Y",time.gmtime(tTime))
                yield(month, price)
        except:
            pass

    def combiner(self, word, values):
        total = 0
        totalPrice = 0
        for val in values:
            total += 1
            totalPrice += val
        yield(word, totalPrice/total)

    def reducer(self, word, values):
        total = 0
        totalPrice = 0
        for val in values:
            total += 1
            totalPrice += val
        yield(word, totalPrice/total)



if __name__ == '__main__':
    Gas.run()
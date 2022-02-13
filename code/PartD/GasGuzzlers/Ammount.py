from mrjob.job import MRJob
import time

class Ammount(MRJob):

    def mapper(self, _, line):
        try:
            fields = line.split(",")
            if len(fields) == 7:
                tTime = int(fields[6])
                gas = int(fields[4])
                month = time.strftime("%B %Y",time.gmtime(tTime))
                yield(month, gas)
        except:
            pass

    def combiner(self, word, values):
        total = 0
        totalGas = 0
        for val in values:
            total += 1
            totalGas += val
        yield(word, totalGas/total)

    def reducer(self, word, values):
        total = 0
        totalGas = 0
        for val in values:
            total += 1
            totalGas += val
        yield(word, totalGas/total)



if __name__ == '__main__':
    Ammount.run()
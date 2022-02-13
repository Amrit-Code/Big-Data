from mrjob.job import MRJob
from mrjob.step import MRStep
import time

class MoneyMade(MRJob):

    def mapper1(self, _, line):
        try:
            fields = line.split(",")
            if len(fields) == 7:
                tTime = int(fields[6])
                val = int(fields[3])
                month = time.strftime("%B %Y",time.gmtime(tTime))
                add = fields[2]
                #if(month == "August 2015" or month == "September 2015"):
                if(month == "February 2016" or month == "March 2016"):
                    yield(add, val)
        except:
            pass

    def combiner1(self, word, values):
        yield(word, sum(values))

    def reducer1(self, word, values):
        yield(None,(word, sum(values)))


    def combiner2(self, _, count):
        sorted_values = sorted(count, reverse = True, key = lambda tup:tup[1])
        i = 1
        for value in sorted_values:
            yield ("top", value)
            i += 1
            if i > 20:
                break

    def reducer2(self, _, values):
        sorted_values = sorted(values, reverse = True, key = lambda tup:tup[1])
        i = 1
        for value in sorted_values:
            yield ("{} - {} ".format(value[0],value[1]), i)
            i += 1
            if i > 20:
                break


    def steps(self):
        return[MRStep(mapper = self.mapper1,
                combiner = self.combiner1,
                reducer = self.reducer1),
                MRStep(combiner = self.combiner2,
                reducer = self.reducer2)
                ]



if __name__ == '__main__':
    MoneyMade.run()

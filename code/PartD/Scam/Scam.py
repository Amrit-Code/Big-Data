from mrjob.job import MRJob
from mrjob.step import MRStep
class Scam(MRJob):

    def mapper1(self, _, line):
        try:
            fields = line.split(",")
            if len(fields) == 2:
                yield(fields[1].strip(" "), (1,1))
            elif len(fields) == 7:
                if fields[3].isnumeric():
                    yield(fields[2], (int(fields[3]),2))
                else:
                    for i in range(1, len(fields)):
                        yield(fields[i].strip(" "), (1,1))
            else:
                for i in range(1, len(fields)):
                    yield(fields[i].strip(" "), (1,1))
        except:
            pass

    # def combiner1(self, word, counts):
    #     yield(word, sum(counts))

    def reducer1(self, word, values):
        totalValue = 0
        isScam = False
        for val in values:
            if val[1] == 2:
                totalValue += val[0]
            elif val[1] == 1:
                isScam = True
        if isScam and totalValue > 0:
            yield(None, (word, totalValue))

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
        i = 0
        for value in sorted_values:
            yield ("{} - {} ".format(value[0],value[1]), i)
            i += 1
            if i > 20:
                break


    def steps(self):
        return[MRStep(mapper = self.mapper1,
                #combiner = self.combiner1,
                reducer = self.reducer1),
                MRStep(combiner = self.combiner2,
                reducer = self.reducer2)
                ]

if __name__ == '__main__':
    Scam.run()
from mrjob.job import MRJob
from mrjob.step import MRStep
class Scam(MRJob):

    def mapper1(self, _, line):
        try:
            fields = line.split(", ")
            if len(fields) == 2:
                yield(fields[1], 1)
        except:
            pass

    def combiner1(self, word, counts):
        yield(word, sum(counts))

    def reducer1(self, word, counts):
        yield(None, (word, sum(counts)))

    def combiner2(self, _, count):
        sorted_values = sorted(count, reverse = True, key = lambda tup:tup[1])
        for value in sorted_values:
            yield ("top", value)

    def reducer2(self, _, values):
        sorted_values = sorted(values, reverse = True, key = lambda tup:tup[1])
        i = 0
        for value in sorted_values:
            yield ("{} - {} ".format(value[0],value[1]), i)
            i += 1


    def steps(self):
        return[MRStep(mapper = self.mapper1,
                combiner = self.combiner1,
                reducer = self.reducer1),
                MRStep(combiner = self.combiner2,
                reducer = self.reducer2)
                ]

if __name__ == '__main__':
    Scam.run()
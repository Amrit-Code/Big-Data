from mrjob.job import MRJob
from mrjob.step import MRStep
class CWPartB1(MRJob):

    #conAdd = {}
    
    def mapper1(self, _ , line):
        try:
            fields = line.split(",")
            if len(fields) == 7:
                address = fields[2]
                value = int(fields[3])
                if (value > 0):
                    yield(address, (value,1))
            elif len(fields) == 5:
                yield(fields[0],(1,2))
                # if not(fields[0] in self.conAdd):
                #     self.conAdd[fields[0]] = 1

        except:
            pass

    def reducer1(self, word, counts):
        # if word in self.conAdd:
        #     yield(word, sum(counts))
        # yield(word, sum(counts))

        isContract = False
        addSum = 0
        for value in counts:
            if value[1] == 1:
                addSum += value[0]
            elif value[1] == 2:
                isContract = True
        if isContract and addSum > 0:
            yield(None,(word,addSum))

    def combiner2(self, _, count):
        sorted_values = sorted(count, reverse = True, key = lambda tup:tup[1])
        i = 0
        for value in sorted_values:
            yield ("top", value)
            i += 1
            if i >= 10:
                break

    def reducer2(self, _, values):
        sorted_values = sorted(values, reverse = True, key = lambda tup:tup[1])
        i = 0
        for value in sorted_values:
            yield ("{} - {} ".format(value[0],value[1]),None)
            i += 1
            if i >= 10:
                break

    def steps(self):
        return[MRStep(mapper = self.mapper1,
                reducer = self.reducer1),
                MRStep(combiner = self.combiner2,
                reducer = self.reducer2)]

if __name__ == '__main__':
    CWPartB1.run()
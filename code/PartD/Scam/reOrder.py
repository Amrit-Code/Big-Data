from mrjob.job import MRJob
from mrjob.step import MRStep
class Order(MRJob):

    def mapper1(self, _, line):
        try:
            fields = line.split(", ")
            yield(None,(int(fields[0]),fields[1]))
        except:
            pass


    def reducer1(self, word, values):
        sorted_values = sorted(values, key = lambda tup:tup[0])
        count = 1
        binNo = 1
        for value in sorted_values:
            yield((binNo,value[1].strip("\n")),1)
            count += 1
            if count > 195:
                count = 1
                binNo += 1

    def combiner2(self, word, count):
        yield(word, sum(count))

    def reducer2(self, word, values):
        yield(word, sum(values))


    def steps(self):
        return[MRStep(mapper = self.mapper1,
                reducer = self.reducer1),
                MRStep(combiner = self.combiner2,
                reducer = self.reducer2)
                ]

if __name__ == '__main__':
    Order.JOBCONF= {'mapreduce.job.reduces': '1' }
    Order.run()


# values = []
# with open('scams.csv') as f:
#     for line in f:
#         fields = line.split(", ")
#         val = [int(fields[0]),fields[1]]   
#         values.append(val)


# sorted_values = sorted(values, key = lambda tup:tup[0])

# count = 1
# binNo = 1
# for value in sorted_values:
#     print("{},{} ".format(binNo,value[1].strip("\n")))
#     count += 1
#     if count > 195:
#         count = 1
#         binNo += 1
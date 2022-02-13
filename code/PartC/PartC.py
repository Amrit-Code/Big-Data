from mrjob.job import MRJob
from mrjob.step import MRStep

class PartC(MRJob):
    def mapper1(self,_,line):
        try:
            fields = line.split(',')
            if len(fields) == 9:
                miner = fields[2]
                size = int(fields[4])
                yield(miner,size)
        except:
            pass

    def combiner1(self,word,counts):
        yield(word,sum(counts))

    def reducer1(self,word,counts):
        yield(None,(word,sum(counts)))

    def combiner2(self,_,counts):
        sorted_values = sorted(counts,reverse=True, key=lambda tup:tup[1])
        i=0
        for count in sorted_values:
            yield("top",count)
            i += 1
            if i >= 10:
                break

    def reducer2(self,_,counts):
        sorted_values=sorted(counts,reverse=True,key=lambda tup:tup[1])
        i= 0
        for count in sorted_values:
            yield("{}-{}".format(count[0],count[1]),None)
            i+= 1
            if i >=10:
                break

    def steps(self):
          return [MRStep(mapper=self.mapper1,
                        combiner = self.combiner1,
                          reducer=self.reducer1),
                  MRStep(
                        combiner= self.combiner2,
                          reducer=self.reducer2)]
if __name__ == '__main__':
    PartC.run()
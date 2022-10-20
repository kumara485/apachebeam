import apache_beam as beam

class SplitData(beam.DoFn):
    def processit(self,element):
        return [element.split(",")]
class CountData(beam.DoFn):
    def findlentght(self,element):
        return[len(element)]

def filterit(status):
    return status[4]=='us'
    




pipeline=beam.Pipeline()
readdata= pipeline |'readdatafromtxt'>>beam.io.ReadFromText("D:\\pywork\\myenv\\abc.txt")
splitdata=readdata | 'mydata'>> beam.ParDo(SplitData())
countt=splitdata | 'count' >> beam.Filter(filterit)
lenit=(countt | 'len'>>beam.ParDo(CountData()))
writeit= lenit | 'write'>>beam.io.WriteToText('xyz.txt')

pipeline.run()

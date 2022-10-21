import apache_beam as beam

class Processing(beam.DoFn):
    def process(self, element):
        return [element.split(",")]

pipeline=beam.Pipeline()
readdata= pipeline |'readdatafromtxt'>>beam.io.ReadFromText("D:\\pywork\\myenv\\abc.txt")
splitdata=readdata | 'mydata'>> beam.ParDo(Processing())
group=splitdata |'grouping'>>beam.GroupBy(lambda x : x[4])
out=group|beam.Map(print)


pipeline.run()

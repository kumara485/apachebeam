import apache_beam as beam

pipeline=beam.Pipeline()
readdata= pipeline |'readdatafromtxt'>>beam.io.ReadFromText("D:\\pywork\\myenv\\abc.txt")
splitdata=readdata | 'mydata'>> beam.Map(lambda d:d.split(","))
countt=(splitdata | 'count' >> beam.GroupBy(lambda x :x[4])| beam.Map(print))


pipeline.run()

                

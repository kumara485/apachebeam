import apache_beam as beam

def filtername(name):
    return name[1] =='Tony Pratt'

pipeline=beam.Pipeline()
cleaned=(pipeline |'readdatafromtxt'>>beam.io.ReadFromText("D:\\pywork\\myenv\\abc.txt")
                  |'split the data' >>beam.Map(lambda x:x.split(","))
                  | 'count'>>beam.Filter(filtername)
                  |beam.Map(print)
                  )
pipeline.run()

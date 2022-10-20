import apache_beam as beam

pipeline=beam.Pipeline()
filterdata=(pipeline|"read data from text" >> beam.io.ReadFromText("D:\\pywork\\myenv\\abc.txt",skip_header_lines=1)
                    | "split the record" >> beam.Map(lambda record:record.split(" "))
                    | "filter kumar" >> beam.Filter(lambda k:k[1]=='kumar')
                    | "write to text" >> beam.io.WriteToText("outputfilter.txt")

)
pipeline.run()

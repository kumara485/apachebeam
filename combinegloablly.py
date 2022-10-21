import apache_beam as beam



with beam.Pipeline() as pipeline:
 data=   (
              pipeline|beam.Create([1,3,4,6,77,99,0])
               |beam.CombineGlobally(sum)
              |beam.Map(print))

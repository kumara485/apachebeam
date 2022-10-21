import apache_beam as beam



with beam.Pipeline() as pipeline:
 data=   (
              pipeline|beam.Create({"apple":1,"orange":4,"apple":6,"orange":77,"coco":99})
               |beam.CombinePerKey(min)
              |beam.Map(print))







                

import apache_beam as beam

def combineit(set):
    set.intersection(*(set()))


with beam.Pipeline() as pipeline:
 data1=(pipeline|beam.Create(
[
{"apple","milk"},
 {"orange","milk"},
 {"apple","milk"},
 {"orange","milk"},
 {"coco","milk"}
 ])
 |beam.CombineGlobally(combineit)|beam.Map(print))

              







                

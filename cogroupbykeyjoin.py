import apache_beam as beam



with beam.Pipeline() as pipeline:
 data1=   pipeline|'one'>>beam.Create({"apple":11,"orange":4,"apple":7,"orange":77,"coco":99})
 data2=   pipeline|'tow'>>beam.Create({"apple":14,"orange":14,"apple":16,"orange":717,"coco":9})
 cobinewithdict=({"data1":data1,"data2":data2}
         |'merge'>> beam.CoGroupByKey()
         | 'print'>> beam.Map(print)
 
 )
 

              







                

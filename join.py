import MapReduce
import sys

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: order id
    # value: record
    key = record[1]
    value = record 

    mr.emit_intermediate(key, value)

def reducer(key, list_of_values):
    # key: order id
    # value: list of records
    
    for v in list_of_values:
      if (v[0] == "order"):
        val = v
        for v in list_of_values:
          if (v[0] != "order"):
            mr.emit(val+v)

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)


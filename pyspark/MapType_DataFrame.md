# Operations on a MapType column in DataFrame

column.keys() is used to get the keys of this MapType column. The data type of column.keys() is not a list neither a single integer/float number. It can be converted to a list via list(column.keys()).

```
df_tetra_order_map.printSchema()
root
 |-- R1: map (nullable = false)
 |    |-- key: integer
 |    |-- value: float (valueContainsNull = true)
 |-- R2: map (nullable = false)
 |    |-- key: integer
 |    |-- value: float (valueContainsNull = true)

def get_edge(R1, R2):
    edge = []
    if list(R1.keys())[0] > list(R2.keys())[0]:
        edge = [R1, R2]
    else:
        edge = [R2, R1]
        
    return edge

get_edge_udf = udf(get_edge, ArrayType(MapType(IntegerType(), FloatType())))

df_tetra_order_map_add = df_tetra_order_map.withColumn("edge", get_edge_udf(df_tetra_order_map.R1, df_tetra_order_map.R2))
df_tetra_order_map_add.printSchema()

root
 |-- R1: map (nullable = false)
 |    |-- key: integer
 |    |-- value: float (valueContainsNull = true)
 |-- R2: map (nullable = false)
 |    |-- key: integer
 |    |-- value: float (valueContainsNull = true)
 |-- edge: array (nullable = true)
 |    |-- element: map (containsNull = true)
 |    |    |-- key: integer
 |    |    |-- value: float (valueContainsNull = true)

df_tetra_order_map_add_col

[Row(R1={5: 5.0}, R2={1: 1.0}, edge=[{5: 5.0}, {1: 1.0}]),
 Row(R1={1: 1.0}, R2={3: 3.0}, edge=[{3: 3.0}, {1: 1.0}]),
 Row(R1={7: 7.0}, R2={3: 3.0}, edge=[{7: 7.0}, {3: 3.0}]),
 Row(R1={2: 2.0}, R2={1: 1.0}, edge=[{2: 2.0}, {1: 1.0}]),
 Row(R1={0: 0.0}, R2={1: 1.0}, edge=[{1: 1.0}, {0: 0.0}])]
```

# TypeError: unhashable type: 'list'
MapType in PySpark corresponds to the dictionary type in Python. However, the error "TypeError: unhashable type: 'list'" may arise in PySpark when trying to perform operations directly on complex data types like arrays or nested arrays.

For instance, we have a DataFrame storing the EF relation, where the EF relation is a MapType. The key is an edge, and value are faces incident in this edge.

```
def get_partial_EF_from_T(tetra):
    edges = []
    edges.append([tetra[0], tetra[1]])
    edges.append([tetra[0], tetra[2]])
    edges.append([tetra[0], tetra[3]])
    edges.append([tetra[1], tetra[2]])
    edges.append([tetra[1], tetra[3]])
    edges.append([tetra[2], tetra[3]])
    
    faces = []
    faces.append([tetra[0], tetra[1], tetra[2]])
    faces.append([tetra[0], tetra[1], tetra[3]])
    faces.append([tetra[0], tetra[2], tetra[3]])
    faces.append([tetra[1], tetra[2], tetra[3]])
    
    part_EF = {}
    
    for e in edges:
        e_set = set(e)
        for f in faces:
            f_set = set(f)
            is_subset = e_set.issubset(f_set)
            if is_subset: 
                if tuple(e) in part_EF:
                    part_EF[tuple(e)].extend([f])
                else:
                    part_EF[tuple(e)] = [f]
                    
    return part_EF

get_partial_EF_from_T_udf = udf(get_partial_EF_from_T, MapType(ArrayType(IntegerType()), ArrayType(ArrayType(IntegerType()))))
df_tetra_order_t = df_tetra_order.withColumn("tetra", sort_array(F.array("r1", "r2", "r3", "r4"), False))
df_EF_part = df_tetra_order_t.withColumn("EF_part", get_partial_EF_from_T_udf(df_tetra_order_t.tetra)).select("tetra_order","EF_part")
df_EF_part.printSchema()

root
 |-- tetra_order: integer (nullable = true)
 |-- EF_part: map (nullable = true)
 |    |-- key: array
 |    |    |-- element: integer (containsNull = true)
 |    |-- value: array (valueContainsNull = true)
 |    |    |-- element: array (containsNull = true)
 |    |    |    |-- element: integer (containsNull = true)
```
The key of "EF_part" is an array of integers. If we collect() this DataFrame, it will show an error "TypeError: unhashable type: 'list'". An alternative way is to first explode() this MapType column and then collect().

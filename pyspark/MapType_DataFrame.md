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

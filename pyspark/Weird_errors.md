# inner join is not as stable as left join
e.g., we have two DataFrames df_crit_V
df_crit_V consists of onecolumn: Min_ver. result_con_VE consists of two columns: id, component.

Here are the codes:

'
**df_crit_V_con = df_crit_V.join(result_con_VE, on=df_crit_V.Min_ver==result_con_VE.id, how="inner")**
df_crit_V_con = df_crit_V_con.select("component", "Min_ver")
df_crit_V_con.printSchema()

df_crit_V_con_col = df_crit_V_con.collect()
'

The errors encountered:

Py4JJavaError: An error occurred while calling o1240.collectToPython.
: org.apache.spark.SparkException: Job aborted due to stage failure: Task 10 in stage 257.0 failed 4 times, most recent failure: Lost task 10.3 in stage 257.0 (TID 2854, dn6.hdp2.cgis.gshpc.umd.edu, executor 7): java.lang.NullPointerException
	at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIteratorForCodegenStage40.processNext(Unknown Source)
	at org.apache.spark.sql.execution.BufferedRowIterator.hasNext(BufferedRowIterator.java:43)
	at org.apache.spark.sql.execution.WholeStageCodegenExec$$anonfun$11$$anon$1.hasNext(WholeStageCodegenExec.scala:624)
	at scala.collection.Iterator$$anon$11.hasNext(Iterator.scala:409)
	at scala.collection.Iterator$$anon$11.hasNext(Iterator.scala:409)
	at scala.collection.Iterator$$anon$11.hasNext(Iterator.scala:409)
	at scala.collection.Iterator$GroupedIterator.fill(Iterator.scala:1124)
	at scala.collection.Iterator$GroupedIterator.hasNext(Iterator.scala:1130)
	at scala.collection.Iterator$$anon$11.hasNext(Iterator.scala:409)
	at scala.collection.Iterator$class.foreach(Iterator.scala:891)
	at scala.collection.AbstractIterator.foreach(Iterator.scala:1334)
	at org.apache.spark.api.python.PythonRDD$.writeIteratorToStream(PythonRDD.scala:224)
	at org.apache.spark.sql.execution.python.PythonUDFRunner$$anon$2.writeIteratorToStream(PythonUDFRunner.scala:50)
	at org.apache.spark.api.python.BasePythonRunner$WriterThread$$anonfun$run$1.apply(PythonRunner.scala:346)
	at org.apache.spark.util.Utils$.logUncaughtExceptions(Utils.scala:1993)
	at org.apache.spark.api.python.BasePythonRunner$WriterThread.run(PythonRunner.scala:195)

 **However, when we change the join format from inner to left, it works perfectly fine**
 'df_crit_V_con = df_crit_V.join(result_con_VE, on=df_crit_V.Min_ver==result_con_VE.id, how="left")'

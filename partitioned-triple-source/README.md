This is an example of a Spark Data Source implementation which expects files on HDFS
that are CSVs partitioned with the file name of pattern TAG.csv Each file would have
3 columns: Tag, Timestamp, Value

These CSVs can be generated using the python script in the scripts directory and
copied to HDFS. The scala script in the scripts directory can be used to test
the data source.

More details on this example can be found on the guide here:
http://blog.hydronitrogen.com/2015/12/04/writing-a-spark-data-source/

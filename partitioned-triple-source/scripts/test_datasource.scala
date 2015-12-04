import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._

val df = sqlContext.read.format("com.hydronitrogen.blog.examples.partitionedtriplesource").load("/user/hkothari/tmp/partitioned-files")

println("Count over whole dataset")
df.count()

println("Select column distinct count")
df.select("tag").distinct().count()

println("Select count one tag using sql where")
df.where("tag = 'A1'").count()

println("Select count three tags using sql where in")
df.where("tag in ('A1', 'K5', 'B2')").count()

println("Count using rdd filter")
df.rdd.filter({a => a(0) == "A1"}).count()

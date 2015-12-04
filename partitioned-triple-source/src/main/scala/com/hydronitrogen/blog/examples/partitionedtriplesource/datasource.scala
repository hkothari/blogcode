package com.hydronitrogen.blog.examples.partitionedtriplesource

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

class DefaultSource extends RelationProvider with DataSourceRegister {

  override def shortName(): String = "partitioned-triple"

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val path = parameters.get("path")
    path match {
        case Some(p) => new PartitionedTripleRelation(sqlContext, p)
        case _ => throw new IllegalArgumentException("Path is required for PartitionedTriple datasets")
    }
  }
}

class PartitionedTripleRelation(val sqlContext: SQLContext, path: String) extends BaseRelation
    with PrunedFilteredScan with Logging {

    def inputFiles: Array[String] = FileSystem.get(sqlContext.sparkContext.hadoopConfiguration)
                                            .listStatus(new Path(path)).map({a => a.getPath().getName()})

    override def schema = StructType(
        StructField("tag", StringType, false) ::
        StructField("timestamp", LongType, false) ::
        StructField("value", DoubleType, false) :: Nil )

    override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
        logInfo(s"Building scan for path: $path")
        var tags: Set[String] = inputFiles.map({a => a.substring(0, a.length - 4)}).toSet
        logInfo(s"Found ${tags.size} tags from path.")

        // Because of our naming scheme we can only do stuff with the tag related
        // filters. All the filters will still be applied at execution so we can
        // safely ignore the others.
        filters.foreach { f =>
            f match {
                case EqualTo(attr, value) => tags = tags filter {a => attr != "tag" || a == value}
                case In(attr, values) => tags = tags filter {a => attr != "tag" || values.toSet.contains(a)}
                case StringStartsWith(attr, value) => tags = tags filter {a => attr != "tag" || a.startsWith(value)}
                case StringEndsWith(attr, value) => tags = tags filter {a => attr != "tag" || a.endsWith(value)}
                case StringContains(attr, value) => tags = tags filter {a => attr != "tag" || a.contains(value)}
            }
        }
        val paths = tags.map {a => new Path(path, a + ".csv").toString()}
        logInfo(s"Will read ${tags.size} files based on filters provided.")

        val rawRdd = sqlContext.sparkContext.textFile(paths.mkString(","))
        val parsedRdd = rawRdd.map { r =>
            val split = r.split(",")
            val tag = split(0)
            val timestamp = split(1).toLong
            val value = split(2).toDouble
            val rowSeq = requiredColumns.map {a =>
                a match {
                    case "tag" => tag
                    case "timestamp" => timestamp
                    case "value" => value
                }
            }
            Row.fromSeq(rowSeq)
        }
        return parsedRdd
    }
}


package com.spark.trade.engine.code

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.io.IOException

object WriteDataFile {

def writeDataFile(spark:SparkSession, finalDataFrame:DataFrame, output:String): Unit = {

  if(!finalDataFrame.isEmpty){
    if(!output.isEmpty) {
      println("********************************"+output)

      var temp = output.split("[.]",0)(0).toString().concat("-temp.csv")
      finalDataFrame.repartition(1).write.option("header", value = false)
        .mode(SaveMode.Overwrite)
        .csv(temp)

      //Call file merge operation
      mergeFiles(spark, temp, output)
    }
    else{ println("OutputFileName not correct"+ output)}
}
  else{
    println("DataFrame is empty, no data or not processed")
  }
}

def mergeFiles(spark: SparkSession, src: String, dest: String) = {
  val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
  if (fs.exists(new Path(src))) {
    if (fs.exists(new Path(dest))) {
      fs.delete(new Path(dest), true)
    }
    copyMerge(fs, new Path(src), fs, new Path(dest), true, spark.sparkContext.hadoopConfiguration)
  }
  else
    println(s"$src File doesn't exists.Nothing to archive")
}

def copyMerge(
               srcFS: FileSystem, srcDir: Path,
               dstFS: FileSystem, dstFile: Path,
               deleteSource: Boolean, conf: Configuration
             ): Boolean = {

  if (dstFS.exists(dstFile)) {
    throw new IOException(s"Target $dstFile already exists")
  }

  // Source path is expected to be a directory:
  if (srcFS.getFileStatus(srcDir).isDirectory) {

    val outputFile = dstFS.create(dstFile)
    try {
      srcFS
        .listStatus(srcDir)
        .sortBy(_.getPath.getName)
        .collect {
          case status if status.isFile =>
            val inputFile = srcFS.open(status.getPath)
            try {
              IOUtils.copyBytes(inputFile, outputFile, conf, false)
            }
            finally {
              inputFile.close()
            }
        }
    } finally {
      outputFile.close()
    }

    if (deleteSource) srcFS.delete(srcDir, true) else true
  }
  else false
}
}
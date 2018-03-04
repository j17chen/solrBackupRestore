package com.hortonworks.support

import java.util.UUID

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
//import sqlContext.implicits._

import com.lucidworks.spark.util.SolrSupport
import org.apache.solr.common.SolrInputDocument



//Usage: solrDataBackupRestore

object solrDataBackupRestore {
  var FIXED_ARGUMENT_SIZE=4
  var ACTION_BACKUP="backup"
  var ACTION_RESTORE="restore"
  var ACTION_GENDATA="gendata"

  var SOLR_ZK_URL=""
  var SOLR_HDFS_PATH=""
  var ACTION=""
  var COLLECTION_NAME=""

  var BATCH_SIZE="1000"
  var SPLITS_PER_SHARD="1"

  var LOOP_COUNT=100;

  def printUsage(): Unit ={
    val usage = """
    Usage: com.hortonworks.support.solrDataBackupRestore <SOLR_ZOOKEEPER_URL> <COLLECTION_NAME> <HDFS_PATH> <ACTION> [--batch-size 100|num] [--splits-per-shard 1|num] [--loop_count 100|num]
    SOLR_ZOOKEEPER_URL: Solr Zookeeper Url. ex: localhost:2181/solr
 |  SOLR_HDFS_PATH: HDFS Path to save or restore solr document
    ACTION:[backup|restore|gendata]
  """
    println(usage)
  }


  def printVar(): Unit ={
    println("SOLR_ZK_URL:"+SOLR_ZK_URL)
    println("COLLECTION_NAME:"+COLLECTION_NAME)
    println("SOLR_HDFS_PATH:"+SOLR_HDFS_PATH)
    println("ACTION:"+ACTION)
    println("BATCH_SIZE:"+BATCH_SIZE)
    println("SPLITS_PER_SHARD:"+SPLITS_PER_SHARD)
    println("LOOP_COUNT:"+LOOP_COUNT)
  }

  def parseOption(list: List[String]) : Unit = {
    def isSwitch(s : String) = (s(0) == '-')

    list match {
      case Nil => return
      case "--batch-size" :: value :: tail =>
        BATCH_SIZE=value
        parseOption(tail)
      case "--splits-per-shard" :: value :: tail =>
        SPLITS_PER_SHARD=value
        parseOption(tail)
      case "--loop_count" :: value :: tail =>
        LOOP_COUNT=value.toInt
        parseOption(tail)
      case string :: opt2 :: tail if isSwitch(opt2) =>
        parseOption(list.tail)
      case string :: Nil =>  parseOption(list.tail)
      case option :: tail => println("Unknown option "+option)
        printUsage()
        sys.exit(1)
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("  Not enough arguments")
      printUsage()
      sys.exit(-1)
    }
    //parse main arguments
    SOLR_ZK_URL=args(0)
    COLLECTION_NAME=args(1)
    SOLR_HDFS_PATH=args(2)
    val strAction=args(3)

    if (strAction.compareTo(ACTION_BACKUP) ==0){
      ACTION=strAction
    }else
    if (strAction.compareTo(ACTION_RESTORE) ==0){
      ACTION=strAction
    }else
    if (strAction.compareTo(ACTION_GENDATA)==0){
      var ACTION_GENDATA=strAction
    }
    else{
      println("Invalid action")
      printUsage()
      sys.exit(-1)
    }

    //parse other options
    val arglist = args.slice(FIXED_ARGUMENT_SIZE,args.length).toList
    parseOption(arglist)
    //print variable
    printVar()

    //initialize spark context and execute

    val conf = new SparkConf().setAppName("solrDataBackupRestore("+ACTION+")")
    val sc=    new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    if (strAction.compareTo(ACTION_BACKUP) ==0){
      println("Perform backup")
      //do backup
      val df = sqlContext.read.format("solr").options(
        Map("zkHost" -> SOLR_ZK_URL, "collection" -> COLLECTION_NAME,"splits" ->"true", "split_field" ->"id","splits_per_shard" ->SPLITS_PER_SHARD,"rows"->BATCH_SIZE)
      ).load();
      df.write.parquet(SOLR_HDFS_PATH)
    }
    if (strAction.compareTo(ACTION_RESTORE)==0)
    {
      println("Restore from backup")
      var batch_size=BATCH_SIZE.toInt
      //do restore
      val dfsrc=sqlContext.read.parquet(SOLR_HDFS_PATH)
      val fields = dfsrc.columns
      val dfsolr=dfsrc.map { row =>
        val document = new SolrInputDocument()
        for (fieldIterator <- 0 until row.length) {
          val fieldName = fields(fieldIterator)
          if (fieldName.compareTo("_version_") !=0){
            document.addField(fields(fieldIterator), row.get(fieldIterator))
          }
        }
        document
      }
      SolrSupport.indexDocs(SOLR_ZK_URL, COLLECTION_NAME, batch_size, dfsolr);

    }
    if (strAction.compareTo(ACTION_GENDATA)==0) {
      println("generate data")
      var batch_size=BATCH_SIZE.toInt
      //do restore
      val dfsrc = sqlContext.read.parquet(SOLR_HDFS_PATH)
      val fields = dfsrc.columns
      val dfsolr = dfsrc.map { row =>
        val document = new SolrInputDocument()
        for (fieldIterator <- 0 until row.length) {
          val fieldName = fields(fieldIterator)
          if (fieldName.compareTo("_version_") != 0) {
            if (fieldName.compareTo("id") == 0) {
              document.addField(fields(fieldIterator), UUID.randomUUID().toString())
            } else {
              document.addField(fields(fieldIterator), row.get(fieldIterator))
            }
          }
        }
        document
      }
      do {
        print("+")
        SolrSupport.indexDocs(SOLR_ZK_URL, COLLECTION_NAME, batch_size, dfsolr);
        LOOP_COUNT=LOOP_COUNT-1
      }while((LOOP_COUNT > 0))

    }

  }
}

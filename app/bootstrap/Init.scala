package bootstrap

//import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import play.api._

object Init extends GlobalSettings {

  //var sparkSession: SparkSession = _

  var sc: SparkContext = _
  var hiveContext: HiveContext = _
  //import hiveContext.implicits._

  /**
   * On start load the json data from conf/data.json into in-memory Spark
   */
  override def onStart(app: Application) {

    /*    SETTING UP SPARK CONFIGURATION   */
    val conf = new SparkConf()
    conf.setAppName("Spark REST Application")
    conf.setMaster("yarn-client")

    sc = new SparkContext(conf)

    /*    SETTING UP HIVE CONTEXT   */
    hiveContext = new HiveContext(sc)


    /*// Spark Conf & hive context
    sparkSession = SparkSession.builder
      .master("local")
      .appName("ApplicationController")
      .getOrCreate()*/

    val dataFrame = hiveContext.read.json("conf/data.json")
    dataFrame.registerTempTable("godzilla")
    //dataFrame.createOrReplaceTempView("godzilla")
  }

  /**
   * On stop clear the sparksession
   */
  override def onStop(app: Application) {

    sc.stop()
  }

  def getSparkSessionInstance = {

    //hiveContext
  }

  def getHiveContext : HiveContext={
    hiveContext
  }

  def getSparkContext = {
    sc
  }
}



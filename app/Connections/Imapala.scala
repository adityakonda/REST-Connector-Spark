package Connections

/**
  * Created by rgajula on 5/16/2018.
  */

import java.sql.Connection
import java.sql.DriverManager
import java.util.Properties

case class imapalconnections(url:String,conprop:Properties)
object Imapala extends App{

  def setImapala():imapalconnections={

    Class.forName("com.cloudera.impala.jdbc4.Driver")

    val jdbcHostname = "<hostname>"
    val jdbcPort = 1433
    val jdbcDatabase ="<database>"

    // Create the JDBC URL without passing in the user and password parameters.
    val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"

    // Create a Properties() object to hold the parameters.
    import java.util.Properties
    val connectionProperties = new Properties()

    var jdbcUsername=""

    var jdbcPassword=""

    connectionProperties.put("user", s"${jdbcUsername}")
    connectionProperties.put("password", s"${jdbcPassword}")

    return imapalconnections(jdbcUrl,connectionProperties)

  }

}

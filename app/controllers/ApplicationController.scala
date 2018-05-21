package controllers

import javax.inject.Inject

import scala.concurrent.Future
import org.apache.spark.sql.{DataFrame}
import play.api.i18n.MessagesApi
import play.api.libs.json.Json
import play.api.mvc.{Action, AnyContent, Controller}
import bootstrap.Init

class ApplicationController @Inject()(implicit webJarAssets: WebJarAssets,
    val messagesApi: MessagesApi) extends Controller {

  def index: Action[AnyContent] = { Action.async {
    val query1 =
      s"""
        SELECT * FROM godzilla WHERE date='2000-02-05' limit 8
      """.stripMargin

     val hiveContext = Init.getHiveContext

      //var imapala=Connections.Imapala.setImapala()
      hiveContext.read.json("conf/data.json")

      val result: DataFrame = hiveContext.sql(query1)
      val rawJson = result.toJSON.collect().mkString
      Future.successful(Ok(Json.toJson(rawJson)))

    }
  }

  def home = Action{

    Ok("Hello World ... !!!")
  }
}




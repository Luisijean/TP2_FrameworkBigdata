package samples.services

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

//Service qui permet aux clients de générer un fichier CSV contenant toutes leurs données, et de leur envoyer par email

object Service3 {

  //On récupère les données de l'utilisateur
  def getClientData(df: DataFrame, id: String, idColumnName: String, writePath: String): Unit = {
    df.filter(col(idColumnName) === id).write.csv(writePath + "_" + id)
  }

}


package samples.services

import java.math.BigInteger
import java.security.MessageDigest

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}

//Service qui permet aux clients de hasher leurs données en fournissant leur identifiant
object Service2 {

  //On définit une udf qui permet de hash un string (masquer le contenu)
  def hashString:UserDefinedFunction = udf((name: String) => {
    val md = MessageDigest.getInstance("MD5")
    val digest = md.digest(name.getBytes())
    val bigInt = new BigInteger(1, digest)
    val hashedString = bigInt.toString(16)
    hashedString
  })

  //Filter with id
  def filterLineWithId(df: DataFrame, id: String, idColumnName: String): DataFrame = {
    df.filter(col(idColumnName) === id)
  }

  //Fonction pour hasher une colonne
  def hashIdColumn(df: DataFrame, columnNameToHash: String, id:String, idColumnName: String): DataFrame = {
    val filteredDf = filterLineWithId(df, id, idColumnName)
    df.withColumn(columnNameToHash, hashString(col(columnNameToHash)))
  }

  //Fonction pour hasher plusieurs colonnes
  //Grace à la methode foldLeft, les transformations seront appliquées successivement au dataframe modifié
  def hashIdColumns(df: DataFrame, columnsNameToHash: Seq[String]): DataFrame = {
    columnsNameToHash.foldLeft(df)((accumulator, current) => accumulator.withColumn(current, hashString(col(current))))
  }
}

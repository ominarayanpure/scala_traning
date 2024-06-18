import org.apache.spark.sql.SparkSession

object CustomDataWriter extends App {

  val sparkSession = SparkSession.builder
    .appName("Cassandra Data Writer")
    .master("local[*]")
    .config("spark.cassandra.connection.host", "cassandra.ap-south-1.amazonaws.com")
    .config("spark.cassandra.connection.port", "9142")
    .config("spark.cassandra.connection.ssl.enabled", "true")
    .config("spark.cassandra.auth.username", "onkar_iam-at-123456789012")
    .config("spark.cassandra.auth.password", "password123")
    .config("spark.cassandra.input.consistency.level", "LOCAL_QUORUM")
    .config("spark.cassandra.connection.ssl.trustStore.path", "/Users/onkar/cassandra_truststore.jks")
    .config("spark.cassandra.connection.ssl.trustStore.password", "onkar@123")
    .getOrCreate()

  // Define a case class corresponding to your schema
  case class SalesData(transaction_id: Int, product_id: Int, product_name: String, customer_name: String, sales_amount: Int)

  val productsDF = sparkSession.read
    .format("csv")
    .option("header", "true")
    .load("/Users/onkar/IdeaProjects/Day17Task/src/main/scala/Products.csv")

  val customersDF = sparkSession.read
    .format("csv")
    .option("header", "true")
    .load("/Users/onkar/IdeaProjects/Day17Task/src/main/scala/Customers.csv")

  val salesDF = sparkSession.read
    .format("csv")
    .option("header", "true")
    .load("/Users/onkar/IdeaProjects/Day17Task/src/main/scala/Sales.csv")

  val productsRenamedDF = productsDF.withColumnRenamed("product_id", "prod_id")
  val customersRenamedDF = customersDF.withColumnRenamed("customer_id", "cust_id").withColumnRenamed("name", "cust_name")

  val salesProductsJoinedDF = salesDF.join(productsRenamedDF, salesDF("product_id") === productsRenamedDF("prod_id"))

  val transactionsWithNamesDF = salesProductsJoinedDF.join(customersRenamedDF, salesProductsJoinedDF("customer_id") === customersRenamedDF("cust_id"))

  val finalSalesAmountDF = transactionsWithNamesDF.select(
    transactionsWithNamesDF("transaction_id"),
    transactionsWithNamesDF("product_id"),
    productsRenamedDF("name").as("product_name"),
    customersRenamedDF("cust_name"),
    (transactionsWithNamesDF("units") * productsRenamedDF("price")).as("total_sales")
  ).toDF("transaction_id", "product_id", "product_name", "customer_name", "total_sales")

  finalSalesAmountDF.show()

  finalSalesAmountDF.write
    .format("org.apache.spark.sql.cassandra")
    .options(Map("table" -> "SalesTable", "keyspace" -> "onkar_keyspace"))
    .mode("append")
    .save()

  sparkSession.stop()
}

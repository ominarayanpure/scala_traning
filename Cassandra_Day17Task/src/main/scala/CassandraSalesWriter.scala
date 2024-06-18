import org.apache.spark.sql.{DataFrame, SparkSession}

object CustomApp extends App {

  val sparkSession: SparkSession = createSparkSession()

  val productsFilePath = "/Users/onkar/IdeaProjects/Day17Task/src/main/scala/Products.csv"
  val customersFilePath = "/Users/onkar/IdeaProjects/Day17Task/src/main/scala/Customers.csv"
  val salesFilePath = "/Users/onkar/IdeaProjects/Day17Task/src/main/scala/Sales.csv"

  val productsDF = readCSV(sparkSession, productsFilePath)
  val customersDF = readCSV(sparkSession, customersFilePath)
  val salesDF = readCSV(sparkSession, salesFilePath)

  val productsRenamedDF = renameProductColumns(productsDF)
  val customersRenamedDF = renameCustomerColumns(customersDF)
  val salesProductsJoinedDF = joinSalesWithProducts(salesDF, productsRenamedDF)
  val finalTransactionsDF = joinSalesWithCustomers(salesProductsJoinedDF, customersRenamedDF)
  val finalSalesAmountDF = calculateSalesAmount(finalTransactionsDF, productsRenamedDF, customersRenamedDF)

  finalSalesAmountDF.show()

  saveToCassandra(finalSalesAmountDF, "SalesTable", "onkar_keyspace")

  sparkSession.stop()

  def createSparkSession(): SparkSession = {
    SparkSession.builder
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
  }

  def readCSV(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .format("csv")
      .option("header", "true")
      .load(path)
  }

  def renameProductColumns(products: DataFrame): DataFrame = {
    products.withColumnRenamed("product_id", "prod_id")
  }

  def renameCustomerColumns(customers: DataFrame): DataFrame = {
    customers.withColumnRenamed("customer_id", "cust_id")
      .withColumnRenamed("name", "cust_name")
  }

  def joinSalesWithProducts(sales: DataFrame, products: DataFrame): DataFrame = {
    sales.join(products, sales("product_id") === products("prod_id"))
  }

  def joinSalesWithCustomers(salesWithProducts: DataFrame, customers: DataFrame): DataFrame = {
    salesWithProducts.join(customers, salesWithProducts("customer_id") === customers("cust_id"))
  }

  def calculateSalesAmount(transactions: DataFrame, products: DataFrame, customers: DataFrame): DataFrame = {
    transactions.select(
      transactions("transaction_id"),
      transactions("product_id"),
      products("name").as("product_name"),
      customers("cust_name"),
      (transactions("units") * products("price")).as("total_sales")
    ).toDF("transaction_id", "product_id", "product_name", "customer_name", "total_sales")
  }

  def saveToCassandra(df: DataFrame, table: String, keyspace: String): Unit = {
    df.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> table, "keyspace" -> keyspace))
      .mode("append")
      .save()
  }
}

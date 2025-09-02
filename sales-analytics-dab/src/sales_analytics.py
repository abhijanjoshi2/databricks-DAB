# Databricks notebook source
# MAGIC %md
# MAGIC # Sales Analytics Pipeline
# MAGIC Simple data processing pipeline for sales data analysis

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, avg, count, when, date_format
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Spark Session and Create Sample Data

# COMMAND ----------

def create_sample_data():
    """Create sample sales data for demonstration"""
    
    # Define schema
    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("sales_date", StringType(), True),
        StructField("region", StringType(), True)
    ])
    
    # Sample data
    sample_data = [
        ("TXN001", "Laptop", "Electronics", 2, 1200.00, "2024-01-15", "North"),
        ("TXN002", "Coffee Maker", "Appliances", 1, 89.99, "2024-01-16", "South"),
        ("TXN003", "Headphones", "Electronics", 3, 150.00, "2024-01-17", "East"),
        ("TXN004", "Desk Chair", "Furniture", 1, 299.99, "2024-01-18", "West"),
        ("TXN005", "Monitor", "Electronics", 2, 350.00, "2024-01-19", "North"),
        ("TXN006", "Blender", "Appliances", 1, 75.50, "2024-01-20", "South"),
        ("TXN007", "Smartphone", "Electronics", 1, 899.99, "2024-01-21", "East"),
        ("TXN008", "Table", "Furniture", 1, 450.00, "2024-01-22", "West"),
        ("TXN009", "Tablet", "Electronics", 2, 299.99, "2024-01-23", "North"),
        ("TXN010", "Microwave", "Appliances", 1, 120.00, "2024-01-24", "South")
    ]
    
    # Create DataFrame
    df = spark.createDataFrame(sample_data, schema)
    
    # Convert sales_date to proper date type
    df = df.withColumn("sales_date", col("sales_date").cast(DateType()))
    df = df.withColumn("total_amount", col("quantity") * col("price"))
    
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Processing Functions

# COMMAND ----------

def analyze_sales_by_category(df):
    """Analyze sales performance by product category"""
    
    category_analysis = df.groupBy("category").agg(
        spark_sum("total_amount").alias("total_revenue"),
        avg("total_amount").alias("avg_transaction_value"),
        count("transaction_id").alias("transaction_count"),
        spark_sum("quantity").alias("total_quantity_sold")
    ).orderBy(col("total_revenue").desc())
    
    return category_analysis

def analyze_sales_by_region(df):
    """Analyze sales performance by region"""
    
    region_analysis = df.groupBy("region").agg(
        spark_sum("total_amount").alias("total_revenue"),
        count("transaction_id").alias("transaction_count"),
        avg("price").alias("avg_product_price")
    ).orderBy(col("total_revenue").desc())
    
    return region_analysis

def get_top_products(df, top_n=5):
    """Get top N products by revenue"""
    
    product_analysis = df.groupBy("product_name").agg(
        spark_sum("total_amount").alias("total_revenue"),
        spark_sum("quantity").alias("total_sold")
    ).orderBy(col("total_revenue").desc()).limit(top_n)
    
    return product_analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Processing Pipeline

# COMMAND ----------

def main():
    """Main data processing pipeline"""
    
    print("=== Sales Analytics Pipeline Started ===")
    
    # Create sample data
    print("Creating sample sales data...")
    sales_df = create_sample_data()
    
    print(f"Total records processed: {sales_df.count()}")
    
    # Show sample of the data
    print("\nSample of sales data:")
    sales_df.show(5)
    
    # Analyze by category
    print("\n=== Sales Analysis by Category ===")
    category_results = analyze_sales_by_category(sales_df)
    category_results.show()
    
    # Analyze by region
    print("\n=== Sales Analysis by Region ===")
    region_results = analyze_sales_by_region(sales_df)
    region_results.show()
    
    # Top products
    print("\n=== Top 5 Products by Revenue ===")
    top_products = get_top_products(sales_df, 5)
    top_products.show()
    
    # Optional: Save results to Unity Catalog (if available)
    try:
        # Uncomment these lines if you want to save to Unity Catalog
        # sales_df.write.mode("overwrite").saveAsTable("main.sales_analytics.raw_sales")
        # category_results.write.mode("overwrite").saveAsTable("main.sales_analytics.category_summary")
        # region_results.write.mode("overwrite").saveAsTable("main.sales_analytics.region_summary")
        print("\nData processing completed successfully!")
    except Exception as e:
        print(f"Note: Could not save to Unity Catalog: {e}")
        print("This is normal if Unity Catalog is not configured.")
    
    print("=== Pipeline Completed ===")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Pipeline

# COMMAND ----------

if __name__ == "__main__":
    main()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Analytics (Optional)

# COMMAND ----------

def advanced_analytics():
    """Additional analytics functions"""
    
    sales_df = create_sample_data()
    
    # Monthly sales trend (limited data, but shows the pattern)
    monthly_trend = sales_df.withColumn("month", date_format("sales_date", "yyyy-MM")) \
        .groupBy("month") \
        .agg(spark_sum("total_amount").alias("monthly_revenue")) \
        .orderBy("month")
    
    print("Monthly Sales Trend:")
    monthly_trend.show()
    
    # High-value transactions (>$500)
    high_value_txns = sales_df.filter(col("total_amount") > 500)
    
    print(f"\nHigh-value transactions (>$500): {high_value_txns.count()}")
    high_value_txns.select("transaction_id", "product_name", "total_amount", "region").show()

# Uncomment to run additional analytics
# advanced_analytics()
import pyspark.sql.functions as F

def model(dbt, session):
    # Explicitly configure the model for Databricks execution
    dbt.config(
        materialized="table",
        catalog="dbtandpyspark1",
        schema="gold",
        submission_method="serverless"  # <-- Tell dbt to use serverless compute for the Python model
    )

    # 1. Pull cleaned data from upstream silver models using dbt.ref()
    customers_df = dbt.ref("silver_customers")
    orders_df = dbt.ref("silver_orders")

    # 2. Perform PySpark transformations & aggregations
    customer_stats = orders_df.groupBy("customer_id").agg(
        F.count("order_id").alias("total_orders"),
        F.sum("amount").alias("lifetime_value"),
        F.avg("amount").alias("avg_order_value"),
        F.max("order_date").alias("last_order_date")
    )

    final_df = customers_df.join(
        customer_stats,
        customers_df["customer_id"] == customer_stats["customer_id"],
        "left"
    ).select(
        customers_df["customer_id"],
        customers_df["customer_name"],
        customers_df["signup_timestamp"],
        F.coalesce(customer_stats["total_orders"], F.lit(0)).alias("total_orders"),
        F.coalesce(customer_stats["lifetime_value"], F.lit(0.0)).alias("lifetime_value"),
        customer_stats["last_order_date"]
    )

    scored_df = final_df.withColumn(
        "is_high_value",
        F.when(F.col("lifetime_value") > 500, True).otherwise(False)
    )

    return scored_df
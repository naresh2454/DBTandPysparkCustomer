import pyspark.sql.functions as F

def model(dbt, session):
    # Configure materialization for this specific Python model
    dbt.config(
        materialized="table",
        catalog="main",
        schema="gold"
    )

    # 1. Pull cleaned data from upstream silver models using dbt.ref()
    customers_df = dbt.ref("silver_customers")
    orders_df = dbt.ref("silver_orders")

    # 2. Perform PySpark DataFrame transformations
    # Aggregate customer order statistics
    customer_stats = orders_df.groupBy("customer_id").agg(
        F.count("order_id").alias("total_orders"),
        F.sum("amount").alias("lifetime_value"),
        F.avg("amount").alias("avg_order_value"),
        F.max("order_date").alias("last_order_date")
    )

    # Join back with customer details
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

    # Add a feature flag (e.g., high-value customer segment)
    scored_df = final_df.withColumn(
        "is_high_value",
        F.when(F.col("lifetime_value") > 500, True).otherwise(False)
    )

    # 3. Return the final PySpark DataFrame back to dbt to materialize in Delta Lake
    return scored_df
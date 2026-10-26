from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import coalesce, col, lit, row_number, when
from pyspark.sql.window import Window

from ecommerce.common_functions.config_utils import get_config, set_logger
from ecommerce.common_functions.delta_utils import create_table, optimize_vacuum_table


def prepare_dim_product_data(
    silver_products: DataFrame, silver_categories: DataFrame
) -> DataFrame:
    # Prepare categories with parent category names via self-join
    cat_df = silver_categories.alias("c")
    parent_cat_df = silver_categories.alias("p")

    categories_enriched = cat_df.join(
        parent_cat_df, col("c.PARENT_CATEGORY_ID") == col("p.CATEGORY_ID"), "left"
    ).select(
        col("c.CATEGORY_ID"),
        coalesce(col("c.CATEGORY_NAME"), lit("Unknown")).alias("CATEGORY_NAME"),
        col("c.PARENT_CATEGORY_ID"),
        coalesce(col("p.CATEGORY_NAME"), lit("Root / None")).alias(
            "PARENT_CATEGORY_NAME"
        ),
    )

    # Join products with enriched categories
    products_joined = silver_products.alias("sp").join(
        categories_enriched.alias("ce"), on="CATEGORY_ID", how="left"
    )

    # Calculate margin safely to avoid division by zero
    margin_expr = when(
        coalesce(col("sp.BASE_PRICE"), lit(0)) > 0,
        (col("sp.BASE_PRICE") - col("sp.COST_PRICE")) / col("sp.BASE_PRICE"),
    ).otherwise(lit(0))

    # Calculate package volume
    volume_expr = (
        coalesce(col("sp.LENGTH_CM"), lit(0))
        * coalesce(col("sp.WIDTH_CM"), lit(0))
        * coalesce(col("sp.HEIGHT_CM"), lit(0))
    )

    dim_product_logic = (
        products_joined.withColumn(
            "PRICE_TIER",
            when(col("sp.BASE_PRICE") < 50, lit("Budget (< 50)"))
            .when(col("sp.BASE_PRICE").between(50, 200), lit("Regular (50-200)"))
            .when(col("sp.BASE_PRICE").between(200, 500), lit("Premium (200-500)"))
            .otherwise(lit("Luxury (> 500)")),
        )
        .withColumn(
            "MARGIN_TIER",
            when(
                (col("sp.BASE_PRICE").isNull())
                | (col("sp.BASE_PRICE") == 0)
                | (margin_expr < 0.15),
                lit("Low Margin (< 15%)"),
            )
            .when(margin_expr.between(0.15, 0.35), lit("Standard Margin (15-35%)"))
            .otherwise(lit("High Margin (> 35%)")),
        )
        .withColumn(
            "WEIGHT_CLASS",
            when(coalesce(col("sp.WEIGHT_G"), lit(0)) < 500, lit("Light (< 0.5 kg)"))
            .when(col("sp.WEIGHT_G").between(500, 5000), lit("Standard (0.5-5 kg)"))
            .when(col("sp.WEIGHT_G").between(5001, 20000), lit("Heavy (5-20 kg)"))
            .otherwise(lit("Freight (> 20 kg)")),
        )
        .withColumn(
            "PACKAGE_SIZE_TIER",
            when(volume_expr < 1000, lit("Small (Parcel A)"))
            .when(volume_expr.between(1000, 5000), lit("Medium (Parcel B)"))
            .when(volume_expr.between(5001, 20000), lit("Large (Parcel C)"))
            .otherwise(lit("Oversized")),
        )
        .withColumn(
            "STOCK_STATUS",
            when(
                coalesce(col("sp.STOCK_QUANTITY"), lit(0)) == 0, lit("Out of Stock (0)")
            )
            .when(col("sp.STOCK_QUANTITY").between(1, 10), lit("Low Stock (1-10)"))
            .when(col("sp.STOCK_QUANTITY").between(11, 100), lit("In Stock (11-100)"))
            .otherwise(lit("High Stock (> 100)")),
        )
        .withColumn(
            "PRODUCT_LIFECYCLE_STATUS",
            when(col("sp.IS_ACTIVE") == 1, lit("Active Catalog")).otherwise(
                lit("Discontinued")
            ),
        )
        .withColumn(
            "SCD_RECORD_STATUS",
            when(col("sp.VALID_TO").isNull(), lit("Current")).otherwise(
                lit("Historical")
            ),
        )
    )

    surrogate_window = Window.orderBy("sp.PRODUCT_ID", "sp.VALID_FROM")

    return dim_product_logic.select(
        row_number().over(surrogate_window).alias("PRODUCT_SURROGATE_KEY"),
        col("sp.PRODUCT_ID"),
        col("sp.PRODUCT_NAME"),
        col("sp.BRAND"),
        col("sp.DESCRIPTION"),
        col("CATEGORY_ID"),
        col("ce.CATEGORY_NAME"),
        col("ce.PARENT_CATEGORY_ID"),
        col("ce.PARENT_CATEGORY_NAME"),
        col("PRICE_TIER"),
        col("MARGIN_TIER"),
        col("WEIGHT_CLASS"),
        col("PACKAGE_SIZE_TIER"),
        col("STOCK_STATUS"),
        col("PRODUCT_LIFECYCLE_STATUS"),
        col("sp.CREATED_AT"),
        col("sp.VALID_FROM"),
        col("sp.VALID_TO"),
        col("SCD_RECORD_STATUS"),
    )


def entry_point_dim_product(spark: SparkSession, env: str, log_level: int) -> None:
    logger = set_logger(log_level=log_level)
    logger.info("Running entry_point for gold dim_product table.")

    config = get_config(env=env, layer="gold", config_filename="dim_product.json")[0]
    table_name_uc = str(
        config.get("table_name_uc") or config.get("table_name") or "dim_product"
    )
    partition_key = config.get("partition_key", [])

    silver_products_df = spark.read.table(
        f"databricks_{env}_domain.ecommerce_silver.products"
    )
    silver_categories_df = spark.read.table(
        f"databricks_{env}_domain.ecommerce_silver.categories"
    )

    dim_product_df = prepare_dim_product_data(
        silver_products=silver_products_df, silver_categories=silver_categories_df
    )

    create_table(
        source_data=dim_product_df,
        table_name_uc=table_name_uc,
        partition_key=partition_key,
    )

    optimize_vacuum_table(spark=spark, table_name_uc=table_name_uc)

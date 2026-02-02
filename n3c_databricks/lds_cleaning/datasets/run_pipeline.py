"""
LDS Cleaning Pipeline - Databricks Entry Point

This script runs the LDS cleaning pipeline on Databricks.
It can be run as a Databricks notebook or as a scheduled job.

Usage in Databricks notebook:
    from n3c_databricks.lds_cleaning.datasets.run_pipeline import run_lds_cleaning_pipeline
    results = run_lds_cleaning_pipeline(spark)
"""

from pyspark.sql import SparkSession
from .lds_clean import clean_all_domains


# =============================================================================
# CONFIGURATION
# =============================================================================
# Update these values to match your Databricks environment

DEFAULT_CONFIG = {
    # Input tables (where unioned OMOP data lives)
    # Format: {input_catalog_schema}.unioned_{domain}
    "input_catalog_schema": "workspace.demo_databricks",

    # Output tables (where cleaned data will be written)
    # Format: {output_catalog_schema}.{domain}
    "output_catalog_schema": "workspace.demo_databricks",

    # Audit tables (where nulled rows are logged)
    # Format: {nulled_rows_catalog_schema}.{domain}_nulled_rows
    "nulled_rows_catalog_schema": "workspace.demo_databricks",
}


def run_lds_cleaning_pipeline(spark: SparkSession, config: dict = None) -> dict:
    """
    Run the LDS cleaning pipeline.

    Args:
        spark: SparkSession
        config: Configuration dictionary. If None, uses DEFAULT_CONFIG.

    Returns:
        Dictionary with results
    """
    if config is None:
        config = DEFAULT_CONFIG

    results = {}

    print("=" * 60)
    print("Cleaning OMOP domains")
    print("=" * 60)

    domain_results = clean_all_domains(
        spark=spark,
        input_catalog_schema=config["input_catalog_schema"],
        output_catalog_schema=config["output_catalog_schema"],
        nulled_rows_catalog_schema=config.get("nulled_rows_catalog_schema")
    )
    results["domains"] = domain_results

    print(f"Completed cleaning {len(domain_results)} domains")

    print("=" * 60)
    print("Pipeline complete!")
    print("=" * 60)

    return results

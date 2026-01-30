"""
LDS Cleaning Pipeline - Databricks Entry Point

This script runs the LDS cleaning pipeline on Databricks.
It can be run as a Databricks notebook or as a scheduled job.

Usage in Databricks notebook:
    %run ./run_pipeline

Or import and call directly:
    from lds_cleaning.datasets.run_pipeline import run_lds_cleaning_pipeline
    run_lds_cleaning_pipeline(spark, config)
"""

from pyspark.sql import SparkSession
from .lds_clean import clean_all_domains
from .manifest_lds_clean import create_manifest


# =============================================================================
# CONFIGURATION
# =============================================================================
# Update these values to match your Databricks environment

DEFAULT_CONFIG = {
    # Input tables (where unioned OMOP data lives)
    # Format: {input_catalog_schema}.unioned_{domain}
    "input_catalog_schema": "workspace.demo",

    # Output tables (where cleaned data will be written)
    # Format: {output_catalog_schema}.{domain}
    "output_catalog_schema": "workspace.demo",

    # Audit tables (where nulled rows are logged)
    # Format: {nulled_rows_catalog_schema}.{domain}_nulled_rows
    "nulled_rows_catalog_schema": "workspace.demo",

    # Manifest input tables
    "manifest_table": "workspace.demo.manifest",
    "data_partners_table": "workspace.demo.data_partners",
    "data_partner_release_status_table": "workspace.demo.data_partner_release_status",
    "check_group_links_table": "workspace.demo.check_group_links",
    "pprl_site_opt_ins_table": "workspace.demo.pprl_site_opt_ins",
    "dedup_sites_table": "workspace.demo.dedup_sites",

    # Manifest output
    "manifest_output_table": "workspace.demo.lds_manifest",
}


def run_lds_cleaning_pipeline(spark: SparkSession, config: dict = None) -> dict:
    """
    Run the complete LDS cleaning pipeline.

    Args:
        spark: SparkSession
        config: Configuration dictionary. If None, uses DEFAULT_CONFIG.

    Returns:
        Dictionary with results from each step
    """
    if config is None:
        config = DEFAULT_CONFIG

    results = {}

    # Step 1: Clean all OMOP domains
    print("=" * 60)
    print("Step 1: Cleaning OMOP domains")
    print("=" * 60)

    domain_results = clean_all_domains(
        spark=spark,
        input_catalog_schema=config["input_catalog_schema"],
        output_catalog_schema=config["output_catalog_schema"],
        nulled_rows_catalog_schema=config.get("nulled_rows_catalog_schema")
    )
    results["domains"] = domain_results

    print(f"Completed cleaning {len(domain_results)} domains")

    # Step 2: Create manifest
    print("=" * 60)
    print("Step 2: Creating manifest")
    print("=" * 60)

    manifest_df = create_manifest(
        spark=spark,
        manifest_table=config["manifest_table"],
        data_partners_table=config["data_partners_table"],
        data_partner_release_status_table=config["data_partner_release_status_table"],
        check_group_links_table=config["check_group_links_table"],
        pprl_site_opt_ins_table=config["pprl_site_opt_ins_table"],
        dedup_sites_table=config["dedup_sites_table"],
        output_table=config["manifest_output_table"]
    )
    results["manifest"] = manifest_df

    print("Manifest created successfully")

    print("=" * 60)
    print("Pipeline complete!")
    print("=" * 60)

    return results


# =============================================================================
# NOTEBOOK ENTRY POINT
# =============================================================================
# When running as a Databricks notebook, uncomment the following:

# if __name__ == "__main__":
#     # Get the SparkSession (automatically available in Databricks as 'spark')
#     # spark = SparkSession.builder.getOrCreate()  # Uncomment if not in Databricks
#
#     # Customize config if needed
#     config = DEFAULT_CONFIG.copy()
#     # config["input_catalog_schema"] = "my_catalog.my_schema"
#
#     # Run the pipeline
#     results = run_lds_cleaning_pipeline(spark, config)

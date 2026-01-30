from pyspark.sql import SparkSession, DataFrame
from typing import Optional, Tuple
from . import utils
from .phi_utils import clean_free_text_cols

# Domains that require PHI cleaning
DOMAINS_FOR_PROCESSING = [
    "condition_occurrence",
    "death",
    "measurement",
    "observation",
    "person",
    "procedure_occurrence",
    "provider",
    "visit_occurrence"
]

# Domains that undergo simple identity transforms (pass-through)
IDENTITY_DOMAINS = [
    "care_site",
    "condition_era",
    "control_map",
    "device_exposure",
    "drug_era",
    "drug_exposure",
    "location",
    "note",
    "note_nlp",
    "observation_period",
    "payer_plan_period",
    "visit_detail"
]


def clean_domain(
    spark: SparkSession,
    domain: str,
    input_table: str,
    output_table: str,
    nulled_rows_table: Optional[str] = None
) -> Tuple[DataFrame, Optional[DataFrame]]:
    """
    Clean a single OMOP domain by removing potential PHI from free-text columns.

    Args:
        spark: SparkSession
        domain: The OMOP domain name (e.g., 'person', 'measurement')
        input_table: Fully qualified input table name (e.g., 'catalog.schema.unioned_person')
        output_table: Fully qualified output table name (e.g., 'catalog.schema.clean_person')
        nulled_rows_table: Optional table name for audit trail of nulled rows

    Returns:
        Tuple of (cleaned_df, nulled_rows_df)
    """
    domain_lower = domain.lower()

    # Configure shuffle partitions for large domains
    if domain_lower == "measurement":
        spark.conf.set('spark.sql.shuffle.partitions', 2000)

    # Read input data
    df = spark.read.table(input_table)

    # Get primary key for this domain
    p_key = utils.DOMAIN_PKEYS.get(domain_lower)

    # Clean free text columns to address PHI
    cleaned_df, nulled_rows_df = clean_free_text_cols(df, domain, p_key)

    # Special handling for person domain
    if domain_lower == "person":
        dedup_cols = ['global_person_id']
        omop_cols = [c for c in cleaned_df.columns if c not in dedup_cols]
        cleaned_df = cleaned_df.select(*omop_cols, *dedup_cols)

    # Write outputs
    cleaned_df.write.mode("overwrite").saveAsTable(output_table)

    if nulled_rows_df is not None and nulled_rows_table is not None:
        nulled_rows_df.write.mode("overwrite").saveAsTable(nulled_rows_table)

    return cleaned_df, nulled_rows_df


def identity_transform(
    spark: SparkSession,
    input_table: str,
    output_table: str
) -> DataFrame:
    """
    Pass-through transform for domains that don't require PHI cleaning.

    Args:
        spark: SparkSession
        input_table: Fully qualified input table name
        output_table: Fully qualified output table name

    Returns:
        The DataFrame (unchanged)
    """
    df = spark.read.table(input_table)
    df.write.mode("overwrite").saveAsTable(output_table)
    return df


def clean_all_domains(
    spark: SparkSession,
    input_catalog_schema: str,
    output_catalog_schema: str,
    nulled_rows_catalog_schema: Optional[str] = None
) -> dict:
    """
    Clean all OMOP domains.

    Args:
        spark: SparkSession
        input_catalog_schema: Catalog.schema prefix for input tables (e.g., 'raw_data.lds_union')
        output_catalog_schema: Catalog.schema prefix for output tables (e.g., 'clean_data.lds')
        nulled_rows_catalog_schema: Optional catalog.schema for nulled rows audit tables

    Returns:
        Dictionary of domain -> (cleaned_df, nulled_rows_df)
    """
    results = {}

    # Process domains that need PHI cleaning
    for domain in DOMAINS_FOR_PROCESSING:
        input_table = f"{input_catalog_schema}.unioned_{domain}"
        output_table = f"{output_catalog_schema}.{domain}"
        nulled_rows_table = None
        if nulled_rows_catalog_schema:
            nulled_rows_table = f"{nulled_rows_catalog_schema}.{domain}_nulled_rows"

        print(f"Processing domain: {domain}")
        results[domain] = clean_domain(
            spark=spark,
            domain=domain,
            input_table=input_table,
            output_table=output_table,
            nulled_rows_table=nulled_rows_table
        )

    # Process identity domains (pass-through)
    for domain in IDENTITY_DOMAINS:
        input_table = f"{input_catalog_schema}.unioned_{domain}"
        output_table = f"{output_catalog_schema}.{domain}"

        print(f"Identity transform for domain: {domain}")
        df = identity_transform(
            spark=spark,
            input_table=input_table,
            output_table=output_table
        )
        results[domain] = (df, None)

    return results

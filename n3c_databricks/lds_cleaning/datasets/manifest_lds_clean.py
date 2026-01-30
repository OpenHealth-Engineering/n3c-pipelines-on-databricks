from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from . import utils


def create_manifest(
    spark: SparkSession,
    manifest_table: str,
    data_partners_table: str,
    data_partner_release_status_table: str,
    check_group_links_table: str,
    pprl_site_opt_ins_table: str,
    dedup_sites_table: str,
    output_table: str
) -> DataFrame:
    """
    Create the LDS manifest dataset with release status and linkage information.

    Args:
        spark: SparkSession
        manifest_table: Input manifest table
        data_partners_table: Data partners reference table
        data_partner_release_status_table: Release status table
        check_group_links_table: Check group links table
        pprl_site_opt_ins_table: PPRL site opt-ins table
        dedup_sites_table: Deduplication sites table
        output_table: Output table for the enriched manifest

    Returns:
        The enriched manifest DataFrame
    """
    # Read input tables
    manifest = spark.read.table(manifest_table)
    data_partners = spark.read.table(data_partners_table)
    data_partner_release_status = spark.read.table(data_partner_release_status_table)
    check_group_links = spark.read.table(check_group_links_table)
    pprl_site_opt_ins = spark.read.table(pprl_site_opt_ins_table)
    dedup_sites = spark.read.table(dedup_sites_table)

    # Remove columns from DTA status check
    dta_cols_to_drop = ["institutionid", "DTA_ACTIVE"]
    manifest = manifest.drop(*dta_cols_to_drop)

    # Cast date columns to date type, handling regular dates and unix timestamps
    manifest = manifest.withColumn("CONTRIBUTION_DATE", manifest["CONTRIBUTION_DATE"].cast("date"))
    manifest = manifest.withColumn(
        "RUN_DATE",
        F.coalesce(
            manifest["RUN_DATE"].cast("date"),
            F.from_unixtime(manifest["RUN_DATE"]).cast("date"),
            F.from_unixtime(F.unix_timestamp(manifest["RUN_DATE"], format='dd-MMM-yy hh.mm.ss.SSSSSSSSS a')).cast("date"),
            F.from_unixtime(F.unix_timestamp(manifest["RUN_DATE"], format='MM/dd/yyyy HH:mm:ss.SSSSSS')).cast("date"),
            F.from_unixtime(F.unix_timestamp(manifest["RUN_DATE"], format='MMM dd yyyy hh:mma')).cast("date"),
            F.from_unixtime(F.unix_timestamp(manifest["RUN_DATE"], format='dd-MMM-yy')).cast('date'),
            F.to_date(manifest["RUN_DATE"], format='yyyy.MM.dd')
        )
    )

    manifest = utils.lower_case_cols(manifest)
    manifest = manifest.withColumn("cdm_name", F.upper(F.col("cdm_name")))
    manifest = manifest.withColumn("old_site_id", manifest["old_site_id"].cast("string"))
    manifest = manifest.withColumn("n3c_site_id", manifest["n3c_site_id"].cast("string"))

    # Add sites that are being tracked but have not yet been processed
    unprocessed = data_partners.join(manifest, "data_partner_id", "left_anti")
    unprocessed = unprocessed.selectExpr(
        "upper(source_cdm) as cdm_name",
        "data_partner_id",
        "original_data_partner_id as old_site_id"
    )
    for col, d_type in manifest.dtypes:
        if (d_type == "string"):
            # Sites are adding extraneous spaces and double quotes to text fields
            manifest = manifest.withColumn(col, F.trim(F.regexp_replace(manifest[col], "[\"]", "")))

        if (col not in unprocessed.columns):
            if (d_type == "string"):
                unprocessed = unprocessed.withColumn(col, F.lit("[Not yet processed]"))
            else:
                unprocessed = unprocessed.withColumn(col, F.lit(None).cast(d_type))
    manifest = manifest.unionByName(unprocessed)

    manifest = manifest.withColumn("processed_in_foundry", F.lit("Y"))

    released_sites = data_partner_release_status.filter(F.col('released') == 'true').selectExpr('site_id AS data_partner_id', 'released')

    manifest = manifest.join(released_sites, 'data_partner_id', 'left').fillna(False, subset=["released"])

    # Handle sites that don't properly submit CDM names. All entries in this column have already been converted to uppercase
    manifest = manifest.withColumn(
        "cdm_name",
        F.when(F.col("cdm_name").contains("PEDSNET"), F.lit("OMOP (PEDSNET)"))\
         .when((F.col("cdm_name").contains("OMOP")) & ~(F.col("cdm_name").contains("PEDSNET")), F.lit("OMOP"))\
         .when(F.col("cdm_name").contains("TRINETX"), F.lit("TRINETX"))\
         .when(F.col("cdm_name").contains("PCORNET"), F.lit("PCORNET"))\
         .when(F.col("cdm_name").contains("ACT"), F.lit("ACT"))
    )

    # Get check group dashboard link for each site
    manifest = manifest.join(check_group_links, "data_partner_id", "left")

    # Add clinical linkage column
    dedup_sites = dedup_sites.withColumn('site_to_site_linkage', F.lit(True))
    manifest = manifest.join(dedup_sites, 'data_partner_id', 'left')
    manifest = manifest.withColumn('site_to_site_linkage', F.when(F.col('site_to_site_linkage').isNull(), F.lit(False)).otherwise(F.col('site_to_site_linkage')))

    # Add CMS, Mortality, Varial Variants and other Linkage columns
    site_ids = data_partners.select('site_name', 'institutionid', 'data_partner_id')
    site_ids = site_ids.filter(~F.col('site_name').contains('TESTING'))
    site_ids = site_ids.withColumnRenamed('site_name', 'institutionname')

    pprl_opt_ins = pprl_site_opt_ins.select('institutionid', 'institutionname', 'feature_name', 'is_participating_yn')
    pprl_opt_ins = pprl_opt_ins.join(site_ids.select('data_partner_id', 'institutionname'), 'institutionname', 'left')
    site_ids = site_ids.withColumnRenamed('data_partner_id', 'data_partner_id_2')

    pprl_opt_ins = pprl_opt_ins.join(site_ids.select('data_partner_id_2', 'institutionid'), 'institutionid', 'left')
    pprl_opt_ins = pprl_opt_ins.withColumn('data_partner_id', F.coalesce(F.col('data_partner_id'), F.col('data_partner_id_2'))).drop('data_partner_id_2')
    pprl_opt_ins = pprl_opt_ins.filter(F.col('data_partner_id').isNotNull())
    pprl_opt_ins = pprl_opt_ins.filter(F.col('is_participating_yn') == 'Yes')
    cms_linkages = pprl_opt_ins.filter(F.col('feature_name') == 'CMS').dropDuplicates()
    mortality_linkages = pprl_opt_ins.filter(F.col('feature_name') == 'Mortality').dropDuplicates()
    viral_variant_linkages = pprl_opt_ins.filter(F.col('feature_name') == 'Viral Variants').dropDuplicates()

    # 'cms_linkage' column
    manifest = manifest.join(cms_linkages.select('feature_name', 'data_partner_id'), 'data_partner_id', 'left')
    manifest = manifest.withColumnRenamed('feature_name', 'cms_linkage')
    manifest = manifest.withColumn('cms_linkage', F.when(F.col('cms_linkage') == 'CMS', F.lit(True)).otherwise(F.lit(False)))

    # 'mortality_linkage' column
    manifest = manifest.join(mortality_linkages.select('feature_name', 'data_partner_id'), 'data_partner_id', 'left')
    manifest = manifest.withColumnRenamed('feature_name', 'mortality_linkage')
    manifest = manifest.withColumn('mortality_linkage', F.when(F.col('mortality_linkage') == 'Mortality', F.lit(True)).otherwise(F.lit(False)))

    # 'viral_variants_linkage' column
    manifest = manifest.join(viral_variant_linkages.select('feature_name', 'data_partner_id'), 'data_partner_id', 'left')
    manifest = manifest.withColumnRenamed('feature_name', 'viral_variants_linkage')
    manifest = manifest.withColumn('viral_variants_linkage', F.when(F.col('viral_variants_linkage') == 'Viral Variants', F.lit(True)).otherwise(F.lit(False)))

    # Write output
    manifest.write.mode("overwrite").saveAsTable(output_table)

    return manifest

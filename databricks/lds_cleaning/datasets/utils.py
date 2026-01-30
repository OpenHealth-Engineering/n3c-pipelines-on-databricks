from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window as W

DOMAINS = [
    "CARE_SITE",
    "CONDITION_ERA",
    "CONDITION_OCCURRENCE",
    "CONTROL_MAP",
    "DEATH",
    "DRUG_ERA",
    "DRUG_EXPOSURE",
    "DEVICE_EXPOSURE",
    "LOCATION",
    "MEASUREMENT",
    "NOTE",
    "NOTE_NLP",
    "OBSERVATION",
    "OBSERVATION_PERIOD",
    "PAYER_PLAN_PERIOD",
    "PERSON",
    "PROCEDURE_OCCURRENCE",
    "PROVIDER",
    "VISIT_DETAIL",
    "VISIT_OCCURRENCE"
]


DOMAINS_TO_FILTER_MULTIPLE_PERSONS_PER_VISIT = [
    "condition_occurrence",
    "procedure_occurrence",
    "measurement",
    "observation",
    "visit_occurrence",
    "drug_exposure",
    "device_exposure"
]


DOMAINS_WITH_PERSON_ID_COLUMN = [
    "CONDITION_ERA",
    "CONDITION_OCCURRENCE",
    "DEATH",
    "DRUG_ERA",
    "DRUG_EXPOSURE",
    "DEVICE_EXPOSURE",
    "MEASUREMENT",
    "NOTE",
    "OBSERVATION",
    "OBSERVATION_PERIOD",
    "PAYER_PLAN_PERIOD",
    "PERSON",
    "PROCEDURE_OCCURRENCE",
    "VISIT_DETAIL",
    "VISIT_OCCURRENCE"
]


DOMAIN_PKEYS = {
    "care_site": "care_site_id",
    "condition_era": "condition_era_id",
    "condition_occurrence": "condition_occurrence_id",
    "control_map": "control_map_id",
    "death": "person_id",
    "drug_era": "drug_era_id",
    "drug_exposure": "drug_exposure_id",
    "device_exposure": "device_exposure_id",
    "location": "location_id",
    "measurement": "measurement_id",
    "note": "note_id",
    "note_nlp": "note_nlp_id",
    "observation": "observation_id",
    "observation_period": "observation_period_id",
    "payer_plan_period": "payer_plan_period_id",
    "person": "person_id",
    "procedure_occurrence": "procedure_occurrence_id",
    "provider": "provider_id",
    "visit_detail": "visit_detail_id",
    "visit_occurrence": "visit_occurrence_id"
}


empty_payer_plan_cols = [
    "payer_source_concept_id",
    "plan_concept_id",
    "plan_source_concept_id",
    "sponsor_concept_id",
    "sponsor_source_concept_id",
    "stop_reason_concept_id",
    "stop_reason_source_concept_id"
]


def conceptualize(domain, df, concept):
    concept = concept.select("concept_id", "concept_name", "concept_class_id")
    concept = concept.persist()

    concept_id_columns = [col for col in df.columns if col.endswith("_concept_id")]
    # _concept_id columns should be Integer
    df = df.select(*
        [col for col in df.columns if col not in concept_id_columns] +
        [F.col(col).cast("integer").alias(col) for col in concept_id_columns]
    )

    broadcasted_concept = F.broadcast(concept)

    for col in concept_id_columns:
        new_df = df
        if col in empty_payer_plan_cols:
            # Create an empty *_concept_name column for these cols to prevent an OOM during the join while keeping the schema consistent
            new_df = new_df.withColumn("concept_name", F.lit(None).cast("string"))
        else:
            new_df = new_df.join(broadcasted_concept, [new_df[col] == concept["concept_id"]], "left_outer").drop("concept_id", "concept_class_id")
        concept_type = col[:col.index("_concept_id")]
        new_df = new_df.withColumnRenamed("concept_name", concept_type+"_concept_name")
        df = new_df

    # occurs in observation, measurement
    if "value_as_number" in df.columns:
        df = df.withColumn("value_as_number", df.value_as_number.cast("double"))

    if "person_id" in df.columns:
        df = df.filter(df.person_id.isNotNull())

    return df


def lower_case_cols(df):
    for c in df.columns:
        df = df.withColumnRenamed(c, c.lower())
    return df


def clean(df):
    for col in df.columns:
        if "decimal" in get_dtype(df, col) and '_id' in col:
            df = df.withColumn(col, df[col].cast(T.IntegerType()))
        elif "decimal" in get_dtype(df, col):
            df = df.withColumn(col, df[col].cast(T.DoubleType()))

        # Parse all date columns (they are given as milliseconds since the epoch)
        if col.endswith("date"):
            df = df.withColumn(col, F.from_unixtime(df[col]/1000).cast(T.DateType()))
        if col.endswith("datetime"):
            df = df.withColumn(col, F.from_unixtime(df[col]/1000).cast(T.TimestampType()))

    return df


def get_dtype(df, col):
    return [dtype for name, dtype in df.dtypes if name == col][0]


# Per NCATS request, for any records in the person table with a race_concept_id = 8657 ("American Indian or Alaska Native"),
# re-map to a race_concept_id = 45878142 ("Other").
# Also, for these rows, replace race_source_value with "Other" and race_source_concept_id with 0.
# tschwab update August 2022 - NCATS asked us to undo this hiding logic
def filter_anai_records(person_df):
    return person_df

    anai_race_concept_id = 8657             # Standard concept: concept_name = "American Indian or Alaska Native", domain_id = "Race"
    other_standard_concept_id = 45878142    # Standard concept: concept_name = "Other", domain_id = "Meas Value"

    person_df = person_df.withColumn(
        "anai_flag",
        F.when(F.col("race_concept_id") == F.lit(anai_race_concept_id), F.lit(1))\
        .otherwise(F.lit(0))
    ).withColumn(
        "race_concept_id",
        F.when(F.col("anai_flag") == F.lit(1), F.lit(other_standard_concept_id))\
        .otherwise(F.col("race_concept_id"))
    ).withColumn(
        "race_source_concept_id",
        F.when(F.col("anai_flag") == F.lit(1), F.lit(0))\
        .otherwise(F.col("race_source_concept_id"))
    ).withColumn(
        "race_source_value",
        F.when(F.col("anai_flag") == F.lit(1), F.lit("Other"))\
        .otherwise(F.col("race_source_value"))
    )

    # Handle records with AN/AI source values that aren't mapped to the AN/AI concept_id
    person_df = person_df.withColumn(
        "race_source_value",
        F.when(F.col("race_source_value").rlike("(?i)(.*indian.*native.*)|(.*indian.*alask.*)|(.*amer.*indian.*)|(.*choctaw.*indian.*)"), F.lit(None))\
        .otherwise(F.col("race_source_value"))
    )
    return person_df.drop("anai_flag")


# Return rows that have a visit_occurrence_id associated with more than 1 person_id
def get_rows_with_multiple_persons_per_visit(domain_df):
    w = W.partitionBy("visit_occurrence_id")
    # collect all rows that have a visit_occurrence_id associated with more than 1 person_id
    bad_rows_df = domain_df.filter(F.col('visit_occurrence_id').isNotNull()) \
        .withColumn('distinct_person_count', F.size(F.collect_set("person_id").over(w)))
    bad_rows_df = bad_rows_df \
        .filter(bad_rows_df.distinct_person_count > 1) \
        .drop('distinct_person_count') \
        .withColumn('removal_reason', F.lit('MULTIPLE_PERSONS_PER_VISIT_OCCURRENCE'))

    return bad_rows_df


# Return rows that have a person_id not found in the person domain
# REQUIRES that both {all_persons_df} and {person_domain_df} have person_id column
# NOTE that this will report 'null' as a missing person if null person_ids exist.
def get_rows_with_missing_persons(all_persons_df, person_domain_df):
    bad_rows_df = all_persons_df.join(person_domain_df, on='person_id', how='left_anti')

    return bad_rows_df.withColumn('removal_reason', F.lit('PERSON_ID_NOT_IN_PERSON_DOMAIN'))


domain_partitions = {
    "care_site": 10,
    "condition_era": 20,
    "condition_occurrence": 100,
    "control_map": 30,
    "death": 10,
    "device_exposure": 10,
    "drug_era": 20,
    "drug_exposure": 250,
    "location": 10,
    "measurement": 3000,
    "note": 10,
    "note_nlp": 15,
    "observation": 150,
    "observation_period": 10,
    "payer_plan_period": 10,
    "person": 10,
    "procedure_occurrence": 80,
    "provider": 10,
    "visit_detail": 20,
    "visit_occurrence": 35,
}

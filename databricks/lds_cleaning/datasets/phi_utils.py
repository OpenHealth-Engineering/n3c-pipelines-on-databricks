from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window as W
from functools import reduce

"""
The following OMOP columns *could* contain free-text entries (although we err on the side of caution and include
many *_source_value columns that are unlikely to contain PHI since they mainly store concept codes). The free text
fields in the non-OMOP source data models all map to columns included in the list below.
- TriNetX
    - LAB_RESULT.TEXT_RESULT_VAL    --> MEASUREMENT.value_source_value and OBSERVATION.value_as_string
- PCORnet
    - LAB_RESULT_CM.raw_result      --> MEASUREMENT.value_source_value
    - OBS_CLIN.obsclin_result_text  --> OBSERVATION.value_as_string
    - OBS_GEN.obsgen_result_text    --> PROCEDURE_OCCURRENCE.procedure_source_value and VISIT_OCCURRENCE.visit_source_value
- ACT
    - OBSERVATION_FACT.tval_char    --> MEASUREMENT.VALUE_SOURCE_VALUE and OBSERVATION.VALUE_AS_STRING
"""
possible_phi_cols = {
    "condition_occurrence": [
        "condition_source_value",
        "condition_status_source_value"
    ],
    "death": [
        "cause_source_value"
    ],
    "measurement": [
        "value_source_value",
        "measurement_source_value"
    ],
    "observation": [
        "value_as_string",
        "observation_source_value",
    ],
    "person": [
        "gender_source_value",
        "race_source_value",
        "ethnicity_source_value",
    ],
    "procedure_occurrence": [
        "procedure_source_value"
    ],
    "provider": [
        "specialty_source_value",
        "gender_source_value"
    ],
    "visit_occurrence": [
        "visit_source_value",
        "admitting_source_value",
        "discharge_to_source_value",
    ]
}

column_values_to_filter = {
    "measurement": {
        "value_source_value": ["NI", "NULL", "OT", "UN"]
    }
}


def union_many(dfs: list) -> DataFrame:
    """Union multiple DataFrames together. Replacement for transforms.verbs.dataframes.union_many"""
    if not dfs:
        return None
    return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)


def clean_free_text_cols(input_df: DataFrame, domain: str, p_key: str) -> tuple:
    """
    Clean free text columns to remove potential PHI.

    Args:
        input_df: Input DataFrame
        domain: The OMOP domain name (e.g., 'person', 'measurement')
        p_key: Primary key column name for this domain

    Returns:
        Tuple of (cleaned_df, nulled_rows_df)
    """
    domain = domain.lower()
    phi_cols = possible_phi_cols.get(domain, [])
    final_domain_df = input_df
    final_nulled_rows_df = None

    # Only apply this cleaning logic to domains with possible PHI columns
    if phi_cols:
        # Reduce the domain to only 2 identifying columns and the possible phi columns
        reduced_input_df = input_df.select(
            p_key,
            "data_partner_id",
            *phi_cols)

        # For efficiency, use lists to store dataframes that need to be unioned/joined after the 'for' loop
        nulled_rows_dfs = []

        for col in phi_cols:
            # Reduce to only 2 identifying columns and the particular column we are investigating.
            # We can filter to non-null values because we will never mark a null column as possible PHI.
            df = reduced_input_df \
                .select(p_key,
                        "data_partner_id",
                        col) \
                .where(F.col(col).isNotNull())

            # if we've got a dict of domain/column/values that we don't want to clean because we know they're clean, lets filter them out
            if domain in column_values_to_filter and col in column_values_to_filter[domain]:
                prefiltered_df = df.filter(~F.col(col).isin(column_values_to_filter[domain][col]))
                full_rows_to_clean_free_text_df = input_df.filter((~F.col(col).isin(column_values_to_filter[domain][col])) & F.col(col).isNotNull())
                full_rows_ignored_free_text_df = input_df.filter((F.col(col).isin(column_values_to_filter[domain][col])) | F.col(col).isNull())
            else:
                prefiltered_df = df
                full_rows_to_clean_free_text_df = input_df
                full_rows_ignored_free_text_df = input_df.limit(0)

            # get a df of distinct col values and their counts
            aggregated_df = prefiltered_df.groupBy(col).count()

            # (1) Create flag for records >= 60 characters in length
            aggregated_df = aggregated_df.withColumn("length_flag", F.length(aggregated_df[col]) >= 60)
            # (2) Create flag for any non-numeric values with a frequency <= 2
            non_numeric_cond = aggregated_df[col].rlike(".*[a-zA-Z]+.*")  # entry cns any letters
            numeric_cond = aggregated_df[col].rlike("^\\d+$")  # entry contains only numbers
            aggregated_df = aggregated_df.withColumn("non_numeric_flag", non_numeric_cond) \
                                         .withColumn("numeric_flag", numeric_cond)

            # Define conditions
            freq_cond = aggregated_df['count'] <= 2
            # Create flags
            aggregated_df = aggregated_df \
                .withColumn("non_numeric_infrequent_flag", freq_cond & non_numeric_cond) \
                .drop('count')

            # Define regex patterns for PHI (Personally Identifiable Information)
            regex_patterns = [
                r"Mr\.",  # Match Mr.
                r"Mrs\.",  # Match Mrs.
                r"\bMiss\b",  # Match Miss
                r"Dr\.",  # Match Dr.
                r", M\.?D\.?",  # Match M.D.
                r"\b\d{3}-\d{2}-\d{4}\b",  # Match Social Security (simplified)
                r"\b\(?([0-9]{3})\)?[-. ]?([0-9]{3})[-. ]([0-9]{4})\b",  # Match US Phone Number
                r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+"  # Match Email
            ]

            # Combine regex patterns using logical OR
            regex_cond = F.lit(False)
            for pattern in regex_patterns:
                regex_cond = regex_cond | aggregated_df[col].rlike(pattern)

            # Add flag for regex matches, excluding numeric-only entries
            aggregated_df = aggregated_df.withColumn("regex_flag",
                                F.when(aggregated_df.numeric_flag, F.lit(False))  # All characters are numeric
                                    .otherwise(regex_cond))

            # Log the rows that were flagged for this column (ie: BAD values)
            aggregated_nulled_rows_df = aggregated_df \
                .where(
                    (aggregated_df["length_flag"] == True) |
                    (aggregated_df["non_numeric_infrequent_flag"] == True) |
                    (aggregated_df["regex_flag"] == True)) \
                .withColumn("column", F.lit(col)) \
                .withColumn("nulled_value", F.col(col)) \

            nulled_rows_df = df \
                .join(aggregated_nulled_rows_df, col, 'inner') \
                .select(
                    "data_partner_id",
                    "column",
                    "nulled_value",
                    "length_flag",
                    "non_numeric_infrequent_flag",
                    "regex_flag"
                )
            nulled_rows_dfs.append(nulled_rows_df)

            # Log the rows that were kept for this column (ie: GOOD values)
            aggregated_preserve_rows_df = aggregated_df \
                .where(
                    (aggregated_df["length_flag"] == False) &
                    (aggregated_df["non_numeric_infrequent_flag"] == False) &
                    (aggregated_df["regex_flag"] == False))

            # this is taking the small number of distinct clean rows and joining them back to the larger set
            preserve_rows_df = prefiltered_df \
                .join(aggregated_preserve_rows_df, col, 'inner') \
                .select(
                    p_key,
                    col
                )

            # drop the col in preparation of joining the cleaned one back in
            full_rows_cleaned_free_text_df = full_rows_to_clean_free_text_df \
                .drop(col)

            # Join back in the possible PHI columns based ONLY on the preserved rows
            # and union in the ignored rows back to input_df so we
            # can do the next cleaning on the next iteration of the for loop
            input_df = full_rows_cleaned_free_text_df \
                .join(preserve_rows_df,
                    on=p_key,
                    how='left') \
                .unionByName(full_rows_ignored_free_text_df)

        # Union all nulled rows using native PySpark
        final_nulled_rows_df = union_many(nulled_rows_dfs)

        final_domain_df = input_df

    return (final_domain_df, final_nulled_rows_df)

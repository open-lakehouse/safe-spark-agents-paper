"""ANSI-safe ingestion helpers (see ANSI_INGESTION.md).

Centralizes the messy-data parsing boundary so every pipeline parses the same way and an agent
does not hand-roll fragile casts. Keep ANSI mode ON; these helpers localize tolerance to the
ingestion boundary (bad value -> NULL, then quarantine), instead of disabling ANSI everywhere.

Usage in a transformation:
    from pipelines._lib.ansi_parse import parse_double, parse_event_time, parse_kafka_json
    df = parse_kafka_json(raw, "order_id string, merchant_id string, event_time string, "
                               "amount string, category string")
    df = df.select("order_id", "merchant_id", parse_event_time("event_time").alias("event_time"),
                   parse_double("amount").alias("amount"), "category")
"""
from pyspark.sql import functions as F, Column


def parse_kafka_json(df, schema_str, value_col="value"):
    """Parse a Kafka value column as JSON with an all-STRING schema. from_json is permissive
    even under ANSI, so a bad field becomes NULL rather than aborting the batch. Coerce types
    afterward with the typed helpers below (never type inside the from_json schema, or epoch-ms
    timestamps get silently nulled)."""
    return df.select(F.from_json(F.col(value_col).cast("string"), schema_str).alias("j")).select("j.*")


def parse_double(col_name) -> Column:
    """ANSI-safe string->double: malformed -> NULL (not a thrown exception)."""
    return F.expr(f"try_cast({col_name} as double)")


def parse_event_time(col_name) -> Column:
    """ANSI-safe timestamp parsing that ALSO handles epoch-millis strings, which
    try_to_timestamp does NOT (it returns NULL on numeric epoch strings). Digit-strings are
    routed through timestamp_seconds; everything else through try_to_timestamp."""
    return F.expr(
        f"CASE WHEN {col_name} rlike '^[0-9]+$' "
        f"THEN timestamp_seconds(cast({col_name} as long) / 1000) "
        f"ELSE try_to_timestamp({col_name}) END"
    )


def quarantine_split(df, required_cols):
    """Split into (clean, quarantine): rows with any NULL in required_cols are quarantined,
    not dropped, so data-quality issues stay visible and auditable. Returns (clean_df, bad_df)."""
    cond = None
    for c in required_cols:
        col_is_null = F.col(c).isNull()
        cond = col_is_null if cond is None else (cond | col_is_null)
    return df.filter(~cond), df.filter(cond)

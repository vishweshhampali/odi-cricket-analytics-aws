# glue_pyspark_app.py  (Glue 4.0/5.0, Spark 3.x)

import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F, types as T
from pyspark.sql.window import Window

# ---------- ARGS ----------
# --INPUT_GLOB s3://my-raw-odi-bucket/odis/raw/*.json
# --MANIFEST_GLOB s3://my-raw-odi-bucket/odis/raw/_manifest/ingest_date=*/manifest_*.jsonl
# --OUTPUT_BASE s3://my-raw-odi-bucket/odis/csv/
# --COALESCE 1
# --READ_MODE latest
# --ACTION_FILTER uploaded
# --FAIL_ON_UNMATCHED true
args = getResolvedOptions(
    sys.argv,
    ["INPUT_GLOB", "MANIFEST_GLOB", "OUTPUT_BASE", "COALESCE",
     "READ_MODE", "ACTION_FILTER", "FAIL_ON_UNMATCHED"]
)
INPUT_GLOB = args["INPUT_GLOB"]
MANIFEST_GLOB = args["MANIFEST_GLOB"]
OUTPUT_BASE = args["OUTPUT_BASE"].rstrip("/") + "/"
COALESCE = int(args["COALESCE"])
READ_MODE = args["READ_MODE"].lower()          # latest | all
ACTION_FILTER = args["ACTION_FILTER"].lower()  # uploaded | skipped | any
FAIL_ON_UNMATCHED = args["FAIL_ON_UNMATCHED"].lower() == "true"

# ---------- SPARK/GLUE ----------
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
spark.sparkContext.setLogLevel("ERROR")

# ---------- SCHEMA ----------
wicket_fielder_schema = T.StructType([ T.StructField("name", T.StringType(), True) ])
wicket_schema = T.StructType([
    T.StructField("player_out", T.StringType(), True),
    T.StructField("kind", T.StringType(), True),
    T.StructField("fielders", T.ArrayType(wicket_fielder_schema), True),
])
runs_schema = T.StructType([
    T.StructField("batter", T.IntegerType(), True),
    T.StructField("extras", T.IntegerType(), True),
    T.StructField("total", T.IntegerType(), True),
])
delivery_schema = T.StructType([
    T.StructField("bowler", T.StringType(), True),
    T.StructField("batter", T.StringType(), True),
    T.StructField("non_striker", T.StringType(), True),
    T.StructField("runs", runs_schema, True),
    T.StructField("wickets", T.ArrayType(wicket_schema), True),
])
over_schema = T.StructType([
    T.StructField("over", T.IntegerType(), True),
    T.StructField("deliveries", T.ArrayType(delivery_schema), True),
])
inning_schema = T.StructType([
    T.StructField("team", T.StringType(), True),
    T.StructField("overs", T.ArrayType(over_schema), True),
])
event_schema = T.StructType([
    T.StructField("name", T.StringType(), True),
    T.StructField("match_number", T.StringType(), True),
    T.StructField("stage", T.StringType(), True),
])
outcome_schema = T.StructType([
    T.StructField("winner", T.StringType(), True),
    T.StructField("result", T.StringType(), True),
])
toss_schema = T.StructType([
    T.StructField("winner", T.StringType(), True),
    T.StructField("decision", T.StringType(), True),
])
info_schema = T.StructType([
    T.StructField("match_type_number", T.LongType(), True),
    T.StructField("event", event_schema, True),
    T.StructField("dates", T.ArrayType(T.StringType()), True),
    T.StructField("match_type", T.StringType(), True),
    T.StructField("season", T.StringType(), True),
    T.StructField("team_type", T.StringType(), True),
    T.StructField("overs", T.IntegerType(), True),
    T.StructField("city", T.StringType(), True),
    T.StructField("venue", T.StringType(), True),
    T.StructField("gender", T.StringType(), True),
    T.StructField("teams", T.ArrayType(T.StringType()), True),
    T.StructField("outcome", outcome_schema, True),
    T.StructField("toss", toss_schema, True),
    T.StructField("players", T.MapType(T.StringType(), T.ArrayType(T.StringType())), True),
])
root_schema = T.StructType([
    T.StructField("info", info_schema, True),
    T.StructField("innings", T.ArrayType(inning_schema), True),
])

# ---------- READ MANIFEST ----------
manifest_raw = (
    spark.read.format("json")
         .load(MANIFEST_GLOB)
         .withColumn("manifest_path", F.input_file_name())
         .select(
             F.col("s3_uri"),
             F.col("stg_file_name"),
             F.col("stg_file_hashkey"),
             F.col("ingested_at"),
             F.col("action"),
             F.col("manifest_path")
         )
)

# keep only latest manifest if requested
if READ_MODE == "latest":
    latest_manifest_path = manifest_raw.agg(F.max("manifest_path").alias("p")).collect()[0]["p"]
    manifest_raw = manifest_raw.filter(F.col("manifest_path") == F.lit(latest_manifest_path))

# filter by action if requested
if ACTION_FILTER in ("uploaded", "skipped"):
    manifest_raw = manifest_raw.filter(F.col("action") == ACTION_FILTER)

# materialize latest row per s3_uri (in case a manifest has duplicates)
w = Window.partitionBy("s3_uri").orderBy(F.col("ingested_at").desc_nulls_last())
manifest_df = (
    manifest_raw
        .withColumn("rn", F.row_number().over(w))
        .where(F.col("rn") == 1)
        .drop("rn")
        .withColumnRenamed("s3_uri", "_source")
)

# ---------- READ JSON ----------
if READ_MODE == "latest":
    # Incremental: only read files listed by the latest manifest (and action filter)
    paths = [r["_source"] for r in manifest_df.select("_source").distinct().collect()]
    if len(paths) == 0:
        raise RuntimeError("No files to process from the selected manifest (check ACTION_FILTER / manifest date).")
    json_df = (
        spark.read.format("json")
             .schema(root_schema)
             .option("multiLine", True)
             .load(paths)
             .withColumn("_source", F.input_file_name())
    )
else:
    # Full refresh: read all JSONs via glob
    json_df = (
        spark.read.format("json")
             .schema(root_schema)
             .option("multiLine", True)
             .load(INPUT_GLOB)
             .withColumn("_source", F.input_file_name())
    )

# ---------- JOIN MANIFEST → STAGING COLUMNS ----------
json_df = (
    json_df.join(manifest_df, on="_source", how="left")
           .withColumn(
               "stg_file_name",
               F.coalesce(
                   F.col("stg_file_name"),
                   F.regexp_extract(F.col("_source"), r'([^/\\]+)$', 1)
               )
           )
           .withColumn("stg_file_row_number", F.lit(1))
           .withColumn("stg_modified_ts", F.current_timestamp())
)

json_df = json_df.withColumn("ingest_date", F.to_date(F.col("ingested_at")))

# ---------- SANITY CHECK ----------
if READ_MODE == "all" and FAIL_ON_UNMATCHED:
    unmatched_cnt = json_df.filter(F.col("stg_file_hashkey").isNull()).limit(1).count()
    if unmatched_cnt > 0:
        raise RuntimeError(
            "Some JSON files had no manifest row. "
            "Either widen MANIFEST_GLOB or set --FAIL_ON_UNMATCHED false."
        )

# ---------- DERIVED ----------
event_date_str = F.element_at(F.col("info.dates"), 1)
event_date = F.to_date(event_date_str, "yyyy-MM-dd")

match_stage = F.coalesce(
    F.col("info.event.match_number").cast("string"),
    F.col("info.event.stage").cast("string"),
    F.lit("NA")
)

match_result = (
    F.when(F.col("info.outcome.winner").isNotNull(), F.lit("Result Declared"))
     .when(F.col("info.outcome.result") == "tie", F.lit("Tie"))
     .when(F.col("info.outcome.result") == "no result", F.lit("No Result"))
     .otherwise(F.col("info.outcome.result"))
)

toss_decision_cap = F.when(
    F.col("info.toss.decision").isNotNull(),
    F.initcap(F.col("info.toss.decision"))
)

# ---------- MATCH TABLE ----------
match_table_df = json_df.select(
    F.col("info.match_type_number").alias("match_type_number"),
    F.col("info.event.name").alias("event_name"),
    match_stage.alias("match_stage"),
    event_date.alias("event_date"),
    F.year(event_date).alias("event_year"),
    F.month(event_date).alias("event_month"),
    F.dayofmonth(event_date).alias("event_day"),
    F.col("info.match_type").alias("match_type"),
    F.col("info.season").alias("season"),
    F.col("info.team_type").alias("team_type"),
    F.col("info.overs").alias("overs"),
    F.col("info.city").alias("city"),
    F.col("info.venue").alias("venue"),
    F.col("info.gender").alias("gender"),
    F.element_at(F.col("info.teams"), 1).alias("first_team"),
    F.element_at(F.col("info.teams"), 2).alias("second_team"),
    match_result.alias("match_result"),
    F.coalesce(F.col("info.outcome.winner"), F.lit("NA")).alias("winner"),
    F.col("info.toss.winner").alias("toss_winner"),
    toss_decision_cap.alias("toss_decision"),
    F.col("stg_file_name"),
    F.col("stg_file_row_number"),
    F.col("stg_file_hashkey"),
    F.col("stg_modified_ts"),
    F.col("ingest_date")
)

# ---------- PLAYER TABLE ----------
players_exploded = (
    json_df
    .select(
        F.col("info.match_type_number").alias("match_type_number"),
        F.explode_outer(F.col("info.players")).alias("country", "players_arr"),
        "stg_file_name", "stg_file_row_number", "stg_file_hashkey", "stg_modified_ts", "ingest_date"
    )
    .withColumn("player_name", F.explode_outer("players_arr"))
    .select(
        "match_type_number", "country", "player_name",
        "stg_file_name", "stg_file_row_number", "stg_file_hashkey", "stg_modified_ts", "ingest_date"
    )
)
player_table_df = players_exploded

# ---------- DELIVERY TABLE ----------
base_deliveries = (
    json_df
    .select(
        F.col("info.match_type_number").alias("match_type_number"),
        F.posexplode_outer("innings").alias("innings_index", "inn")   # <-- capture index
    )
    .withColumn("innings_number", F.col("innings_index") + F.lit(1))  # convert 0-based → 1-based
    .select(
        "match_type_number",
        "innings_number",
        F.col("inn.team").alias("country"),
        F.explode_outer(F.col("inn.overs")).alias("ovr")
    )
    .select(
        "match_type_number",
        "innings_number",
        "country",
        F.col("ovr.over").alias("over"),
        F.posexplode_outer(F.col("ovr.deliveries")).alias("ball_idx_zero", "del")
    )
    .withColumn("ball_in_over", F.col("ball_idx_zero") + F.lit(1))
    .select(
        "match_type_number",
        "innings_number",
        "country",
        "over",
        "ball_in_over",
        F.col("del.bowler").alias("bowler"),
        F.col("del.batter").alias("batter"),
        F.col("del.non_striker").alias("non_striker"),
        F.col("del.runs.batter").alias("runs"),
        F.col("del.runs.extras").alias("extras"),
        F.col("del.runs.total").alias("total"),
        F.explode_outer(F.col("del.wickets")).alias("wkt")
    )
)


fielders_names = F.transform(F.col("wkt.fielders"), lambda f: f.getField("name"))
fielders_joined = F.when(
    F.col("wkt.fielders").isNotNull(),
    F.array_join(fielders_names, ", ")
)

delivery_table_df = (
    base_deliveries
    .select(
        "match_type_number",
        "innings_number",
        "country",
        "over",
        "ball_in_over",
        "bowler",
        "batter",
        "non_striker",
        "runs",
        "extras",
        "total",
        F.col("wkt.player_out").alias("player_out"),
        F.col("wkt.kind").alias("player_out_kind"),
        fielders_joined.alias("player_out_fielders")
    )
    .join(
        json_df.select(
            F.col("_source"),
            F.col("stg_file_name"),
            F.col("stg_file_row_number"),
            F.col("stg_file_hashkey"),
            F.col("stg_modified_ts"),
            F.col("ingest_date"),
            F.col("info.match_type_number").alias("mt_for_join")
        ).dropDuplicates(["_source", "mt_for_join"]),
        on=(F.col("match_type_number") == F.col("mt_for_join")),
        how="left"
    )
    .drop("mt_for_join", "_source")
)

'''
# ---------- WRITE CSVs ----------
def write_csv(df, subpath):
    out = df.coalesce(COALESCE) if COALESCE > 0 else df
    (out.write
        .mode("overwrite")
        .option("header", "true")
        .csv(OUTPUT_BASE + subpath))

write_csv(match_table_df, "match_table/")
write_csv(player_table_df, "player_table/")
write_csv(delivery_table_df, "delivery_table/")
'''
# ---------- WRITE PARQUET ----------

# figure out this run's date from the data you just built
run_date = json_df.agg(F.max("ingest_date").alias("d")).collect()[0]["d"]

def write_parquet_history(df, subpath):
    out = df.coalesce(COALESCE) if COALESCE > 0 else df
    (out.write
        .format("parquet")
        .mode("append")
        .partitionBy("ingest_date")
        .save(OUTPUT_BASE + subpath))

def write_parquet_latest(df, subpath):
    # keep only this run's rows and overwrite a fixed 'latest/' alias
    today = df.filter(F.col("ingest_date") == F.lit(run_date))
    out = today.coalesce(COALESCE) if COALESCE > 0 else today
    (out.write
        .format("parquet")
        .mode("overwrite")
        .save(OUTPUT_BASE + subpath + "latest/"))

# write both history and latest for each table
for sub in ["match_table/", "player_table/", "delivery_table/"]:
    write_parquet_history(eval(sub.split('/')[0] + "_df"), sub)
    write_parquet_latest(eval(sub.split('/')[0] + "_df"), sub)

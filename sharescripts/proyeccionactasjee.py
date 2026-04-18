from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Actas ONPE") \
    .getOrCreate()

df = spark.read.json("E:/1-Apps/Elecciones/proceso/detalleactas.jsonl")

# Actas contabilizadas
df_exploded = df.select(
        F.col("idActa"),
        F.col("data.*")
    ) \
    .filter(F.col('codigoEstadoActa') == 'C') \
    .select(
        "idActa",
        F.col("ubigeoNivel01").alias('region'),
        F.concat_ws(
            "|",
            F.col("nombreLocalVotacion"),
            F.col("ubigeoNivel01"),
            F.col("ubigeoNivel02"),
            F.col("ubigeoNivel03")
        ).alias("ubicacion"),
        F.explode("detalle").alias("detalle_item")
    ) \
    .select(
        "idActa",
        "region",
        "ubicacion",
        F.col("detalle_item.*")
    ) \
    .select(
        "idActa",
        "region",
        "ubicacion",      
        F.col("nvotos"),
        F.when(F.col("nagrupacionPolitica") == 10, "JP")
         .when(F.col("nagrupacionPolitica") == 35, "RP")
         .otherwise("Otros").alias("partido")
    )\
    .filter(F.col("partido").isin("JP", "RP"))

# Base para proyectar
df_base = df_exploded \
    .groupBy("region","ubicacion", "partido") \
    .agg(
        F.count_distinct("idActa").alias("num_actas"),
        F.expr("percentile_approx(nvotos, 0.5)").alias("mediana_votos"),
        F.avg(F.col("nvotos").cast("int")).alias("promedio_votos"),
        F.sum(F.col("nvotos").cast("int")).alias("total_votos")
    )

# Actas en estado "Enviadas a JEE"
df_material = df.select(
        F.col("idActa"),
        F.col("data.*")
    )\
    .filter(F.col('codigoEstadoActa') == 'E') \
    .select(
        "idActa",
        F.col("ubigeoNivel01").alias('region'),
        F.concat_ws(
            "|",
            F.col("nombreLocalVotacion"),
            F.col("ubigeoNivel01"),
            F.col("ubigeoNivel02"),
            F.col("ubigeoNivel03")
        ).alias("ubicacion")
    )

# Imputacion de votos faltantes
df_partidos = df_base.select("partido").distinct()
df_imputado =  df_material.crossJoin(df_partidos) \
    .join(
        df_base.select("region", "ubicacion", "partido", "mediana_votos"),
        on=["region", "ubicacion", "partido"],
        how="left"
    )\
    .select(
        "idActa",
        "region",
        "ubicacion",
        "partido",
        F.col("mediana_votos").alias("nvotos_imputados")
    )

# Salida por region
df_imputado.rollup("region")\
    .agg(
        F.count_distinct("idActa").alias("num_actas_JNE"),
        F.sum(F.when(F.col("partido") == "JP", F.col("nvotos_imputados")).otherwise(0)).alias("total_votos_imputados_JP"),
        F.sum(F.when(F.col("partido") == "RP", F.col("nvotos_imputados")).otherwise(0)).alias("total_votos_imputados_RP")
    )\
    .orderBy("num_actas_JNE", ascending=False)\
    .show(50, truncate=False)
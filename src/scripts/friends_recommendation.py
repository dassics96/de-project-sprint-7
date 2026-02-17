import sys
from datetime import datetime

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.sql.functions import split, regexp_replace, col, trim

def haversine_expr(lat1, lon1, lat2, lon2):
    """Расстояние в км"""
    lat1_rad = F.radians(lat1)
    lon1_rad = F.radians(lon1)
    lat2_rad = F.radians(lat2)
    lon2_rad = F.radians(lon2)

    a = (
        F.pow(F.sin((lat2_rad - lat1_rad) / 2), 2) +
        F.cos(lat1_rad) * F.cos(lat2_rad) *
        F.pow(F.sin((lon2_rad - lon1_rad) / 2), 2)
    )
    c = 2 * F.atan2(F.sqrt(a), F.sqrt(1 - a))
    return F.lit(6371.0) * c


def main():
    # Аргументы:
    #1 — путь к событиям (партиционированным)
    #2 — путь к geo.csv
    #3 — выходной путь витрины рекомендаций
    events_path     = sys.argv[1]   # /user/dburkh/data/geo/events
    geo_path        = sys.argv[2]   # /user/dburkh/data/geo/geo.csv
    output_path     = sys.argv[3]   # /user/dburkh/analytics/friends_rec
    
    geo_path = "/user/dburkh/data/geo/geo.csv"
    
    today = datetime.now().strftime("%Y-%m-%d")

    conf = SparkConf().setAppName(f"FriendsRecommendation-{today}")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    #---------------------------------------------------------------
    #1.Чтение + семпл для отладки
    #---------------------------------------------------------------
    events = sql.read.parquet(events_path)

    # Ограничение объёма — обязательно для этой витрины (self-join + cross)
    events = events.where("date >= '2022-04-01'") \
                   .sample(fraction=0.05, seed=424242)

    print("INFO: using sampled data (5% from 2022-04) for friends recommendation")

    # user_id
    events = events.withColumn("user_id",
        F.coalesce(
            F.col("event.message_from").cast("long"),
            F.col("event.reaction_from").cast("long"),
            F.col("event.subscription_user").cast("long"),
            F.col("event.user").cast("long")
        )
    ).filter(F.col("user_id").isNotNull())

    #---------------------------------------------------------------
    #2.Последние координаты каждого пользователя
    #---------------------------------------------------------------
    last_pos = events.filter(F.col("event_type") == "message") \
        .select(
            "user_id",
            "lat",
            "lon",
            F.to_timestamp(F.col("event.message_ts")).alias("ts")
        ) \
        .withColumn("rn", F.row_number().over(
            Window.partitionBy("user_id").orderBy(F.desc("ts"))
        )) \
        .filter("rn = 1") \
        .drop("rn", "ts") \
        .withColumnRenamed("lat", "last_lat") \
        .withColumnRenamed("lon", "last_lon")

    #---------------------------------------------------------------
    #3.Подписки: user-список каналов
    #---------------------------------------------------------------
    subs = events.filter(F.col("event_type") == "subscription") \
        .select(
            F.col("user_id"),
            F.col("event.subscription_channel").alias("channel_id")
        ) \
        .distinct()

    # explode для self-join
    subs_exploded = subs.select("user_id", "channel_id")

    #---------------------------------------------------------------
    #4. Пары пользователей с хотя бы одним общим каналом
    # ──────────────────────────────────────────────────────────────
    common_subs = subs_exploded.alias("a") \
        .join(subs_exploded.alias("b"),
              (F.col("a.channel_id") == F.col("b.channel_id")) &
              (F.col("a.user_id") < F.col("b.user_id")),   # уникальные пары
              "inner") \
        .select(
            F.col("a.user_id").alias("user_left"),
            F.col("b.user_id").alias("user_right"),
            F.col("a.channel_id")
        ) \
        .distinct()

    #---------------------------------------------------------------
    #5.Исключаем пары, которые уже переписывались (anti-join)
    #---------------------------------------------------------------
    messages_dir = events.filter(F.col("event_type") == "message") \
        .select(
            F.col("event.message_from").cast("long").alias("from"),
            F.col("event.message_to").cast("long").alias("to")
        ) \
        .filter(F.col("from").isNotNull() & F.col("to").isNotNull())

    # bidirectional
    pairs_msg = messages_dir.select(
        F.least("from", "to").alias("u1"),
        F.greatest("from", "to").alias("u2")
    ).distinct()

    candidates = common_subs.join(
        pairs_msg,
        (F.col("user_left") == F.col("u1")) & (F.col("user_right") == F.col("u2")),
        "left_anti"
    )

    #---------------------------------------------------------------
    #6. Присоединяем координаты обоих пользователей
    #---------------------------------------------------------------
    candidates_with_geo = candidates \
        .join(last_pos.alias("l"), F.col("user_left") == F.col("l.user_id"), "inner") \
        .join(last_pos.alias("r"), F.col("user_right") == F.col("r.user_id"), "inner") \
        .withColumn("distance_km", haversine_expr(
            F.col("l.last_lat"), F.col("l.last_lon"),
            F.col("r.last_lat"), F.col("r.last_lon")
        )) \
        .filter(F.col("distance_km") <= 1.0)

    #---------------------------------------------------------------
    #7. Определяем zone_id — берём город user_left (по последним координатам)
    #---------------------------------------------------------------
     # 1. Читаем без header — все данные в одной колонке Читаем без header — Spark создаст колонку _c0
    raw = sql.read.option("header", "false").csv(geo_path)

    # Разбиваем _c0 по ";" на четыре колонки
    cities = raw.withColumn("id",   split("_c0", ";").getItem(0)) \
            .withColumn("city", split("_c0", ";").getItem(1)) \
            .withColumn("lat",  split("_c0", ";").getItem(2)) \
            .withColumn("lng",  split("_c0", ";").getItem(3)) \
            .drop("_c0")

    # Теперь переименовываем lat/lng
    cities = cities.withColumnRenamed("lat", "orig_lat") \
               .withColumnRenamed("lng", "orig_lng")

    # Очищаем запятые и приводим к double
    cities = cities.withColumn("city_lat", F.regexp_replace(F.col("orig_lat"), ",", ".").cast("double"))
    cities = cities.withColumn("city_lon", F.regexp_replace(F.col("orig_lng"), ",", ".").cast("double"))

    # Финальный отбор и приведение типов
    cities = cities.select(
    F.col("id").cast("string").alias("city_id"),
    F.col("city"),
    F.col("city_lat"),
    F.col("city_lon")
)

    cities_bc = F.broadcast(cities)

    # ближайший город для user_left
    left_geo = candidates_with_geo.select(
        "user_left", "user_right", "distance_km",
        F.col("l.last_lat").alias("lat"),
        F.col("l.last_lon").alias("lon")
    )

    with_city = left_geo.crossJoin(cities_bc) \
        .withColumn("dist_to_city", haversine_expr("lat", "lon", "city_lat", "city_lon"))

    w_city = Window.partitionBy("user_left", "user_right").orderBy("dist_to_city")

    final_pairs = with_city.withColumn("rn", F.row_number().over(w_city)) \
                           .filter("rn = 1") \
                           .select(
                               "user_left",
                               "user_right",
                               F.col("city_id").alias("zone_id"),
                               F.current_timestamp().alias("processed_dttm")
                           )

    #---------------------------------------------------------------
    #8. Сохранение
    #---------------------------------------------------------------
    final_pairs.write \
               .mode("overwrite") \
               .partitionBy("processed_dttm") \
               .format("parquet") \
               .save(output_path)

    sc.stop()


if __name__ == "__main__":
    main()
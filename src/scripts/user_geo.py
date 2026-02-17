import sys
from datetime import datetime

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.sql.types import StringType
from pyspark.sql.functions import split


def haversine_expr(lat1_col, lon1_col, lat2_col, lon2_col):
    """Haversine formula в чистом Spark SQL без UDF"""
    lat1 = F.radians(lat1_col)
    lon1 = F.radians(lon1_col)
    lat2 = F.radians(lat2_col)
    lon2 = F.radians(lon2_col)

    a = (
        F.pow(F.sin((lat2 - lat1) / 2), 2) +
        F.cos(lat1) * F.cos(lat2) *
        F.pow(F.sin((lon2 - lon1) / 2), 2)
    )
    c = 2 * F.atan2(F.sqrt(a), F.sqrt(1 - a))
    return F.lit(6371.0) * c  # расстояние в км


def get_timezone(city_col):
    """Маппинг города → timezone (Australia/...)"""
    return (
        F.when(city_col == "Sydney", "Australia/Sydney")
         .when(city_col == "Melbourne", "Australia/Melbourne")
         .when(city_col == "Brisbane", "Australia/Brisbane")
         .when(city_col == "Perth", "Australia/Perth")
         .when(city_col == "Adelaide", "Australia/Adelaide")
         .when(city_col == "Gold Coast", "Australia/Sydney")
         .when(city_col == "Canberra", "Australia/Sydney")
         .when(city_col == "Hobart", "Australia/Hobart")
         .when(city_col == "Darwin", "Australia/Darwin")
         .when(city_col == "Townsville", "Australia/Brisbane")
         .when(city_col == "Cairns", "Australia/Brisbane")
         .otherwise("Australia/Sydney")  # fallback
    )


def main():
    # Аргументы командной строки
    # Пример запуска:
    # spark-submit user_geo.py /user/master/data/geo/events /user/dburkh/data/geo/events /user/dburkh/analytics/user_geo
    source_events_path   = sys.argv[1]   # исходные события
    target_events_path   = sys.argv[2]   # куда переложить с партицией
    output_vitrina_path  = sys.argv[3]   # путь для витрины
    
    geo_path = "/user/dburkh/data/geo/geo.csv"
    
    today = datetime.now().strftime("%Y-%m-%d")

    conf = SparkConf().setAppName(f"UserGeoVitrina-{today}")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    #----------------------------------------------------------------------------
    # 1. Чтение исходных событий + ограничение данных для отладки
    #----------------------------------------------------------------------------
    events_raw = sql.read.parquet(source_events_path)

    # Ограничение объёма данных
    events_raw = events_raw.where("date >= '2022-04-01'") \
                           .sample(fraction=0.10, seed=20232023)

    print("INFO: using sampled data (date >= 2022-04-01 + 10% sample)")

    # Извлекаем user_id из разных возможных полей
    events = events_raw.withColumn(
        "user_id",
        F.coalesce(
            F.col("event.message_from").cast("long"),
            F.col("event.reaction_from").cast("long"),
            F.col("event.subscription_user").cast("long"),
            F.col("event.user").cast("long")
        )
    )

    # Приводим timestamp в единый столбец (UTC)
    events = events.withColumn(
        "event_time_utc",
        F.coalesce(
            F.to_timestamp(F.col("event.message_ts")),
            F.to_timestamp(F.col("event.datetime")),
            F.to_timestamp(F.concat(F.col("date"), F.lit(" 00:00:00")))
        )
    )

    # Фильтруем только полезные записи
    events = events.filter(
        F.col("user_id").isNotNull() &
        F.col("lat").isNotNull() &
        F.col("lon").isNotNull() &
        F.col("event_time_utc").isNotNull()
    )

    #----------------------------------------------------------------------------
    #2. Перекладываем события в новую директорию с партицией по event_type
    #----------------------------------------------------------------------------
    events.write \
          .mode("overwrite") \
          .partitionBy("event_type") \
          .format("parquet") \
          .save(target_events_path)

    #----------------------------------------------------------------------------
    #3. Чтение городов (geo.csv)
    #----------------------------------------------------------------------------

    raw = sql.read.option("header", "false").csv(geo_path)

    cities = raw.withColumn("id",   split("_c0", ";").getItem(0)) \
                .withColumn("city", split("_c0", ";").getItem(1)) \
                .withColumn("lat",  split("_c0", ";").getItem(2)) \
                .withColumn("lng",  split("_c0", ";").getItem(3)) \
                .drop("_c0")

    cities = cities.withColumnRenamed("lat", "orig_lat") \
                   .withColumnRenamed("lng", "orig_lng")

    cities = cities.withColumn("city_lat", F.regexp_replace(F.col("orig_lat"), ",", ".").cast("double"))
    cities = cities.withColumn("city_lon", F.regexp_replace(F.col("orig_lng"), ",", ".").cast("double"))

    cities = cities.select(
        F.col("id").cast("string").alias("city_id"),
        F.col("city"),
        F.col("city_lat"),
        F.col("city_lon")
    )

    #----------------------------------------------------------------------------
    #4. Определение ближайшего города для каждого события
    #----------------------------------------------------------------------------
    cities_bc = F.broadcast(cities)

    with_distance = events.crossJoin(cities_bc) \
        .withColumnRenamed("city", "city_from_geo") \
        .withColumn("distance_km", haversine_expr("lat", "lon", "city_lat", "city_lon"))

    w_nearest = Window.partitionBy("user_id", "event_time_utc").orderBy("distance_km")

    geo_events = with_distance.withColumn("rn", F.row_number().over(w_nearest)) \
                              .filter("rn = 1") \
                              .drop("rn", "distance_km", "city_lat", "city_lon") \
                              .select(
                                  "user_id",
                                  "event_time_utc",
                                  "event_type",
                                  "date",
                                  F.col("city_from_geo").alias("city"),
                                  "city_id"
                              )

    #----------------------------------------------------------------------------
    #5. Метрики путешествий
    #----------------------------------------------------------------------------

    # Окно для упорядочивания событий пользователя по времени
    w_travel = Window.partitionBy("user_id").orderBy("event_time_utc")

    travel_metrics = geo_events.withColumn(
    "rn", F.row_number().over(w_travel)).groupBy("user_id").agg(
    F.count("*").alias("travel_count"),
    F.collect_list("city").alias("travel_array_raw")).withColumn(
    "travel_array",
    F.filter("travel_array_raw", lambda x: x.isNotNull())).drop("travel_array_raw")

#----------------------------------------------------------------------------
#6. Последнее событие пользователя — город, локальное время, timezone
#----------------------------------------------------------------------------

    w_last = Window.partitionBy("user_id").orderBy(F.desc("event_time_utc"))

    last_event_enriched = geo_events.withColumn("rn", F.row_number().over(w_last)) \
    .filter("rn = 1") \
    .drop("rn") \
    .withColumn("timezone", get_timezone(F.col("city"))) \
    .withColumn(
        "local_time",
        F.from_utc_timestamp(F.col("event_time_utc"), F.col("timezone"))
    ) \
    .select(
        "user_id",
        F.col("city").alias("act_city"),
        "local_time"
    )

   # ----------------------------------------------------------------------------
#7. Итоговая витрина — объединяем с метриками путешествий
# ----------------------------------------------------------------------------

    vitrina = last_event_enriched \
    .join(travel_metrics, "user_id", "left_outer") \
    .withColumn("calc_date", F.lit(today)) \
    .withColumn("travel_count", F.coalesce(F.col("travel_count"), F.lit(0))) \
    .withColumn("travel_array", F.coalesce(F.col("travel_array"), F.array())) \
    .select(
        "user_id",
        "act_city",
        "local_time",
        "travel_count",
        "travel_array",
        "calc_date"
    )

    # Сохранение витрины
    vitrina.write \
           .mode("overwrite") \
           .partitionBy("calc_date") \
           .format("parquet") \
           .save(output_vitrina_path)

    print(f"Витрина сохранена: {output_vitrina_path}/calc_date={today}")

    sc.stop()


if __name__ == "__main__":
    main()
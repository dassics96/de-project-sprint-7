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
    #Аргументы командной строки
    #Пример как запускать:
    #spark-submit user_geo.py /user/master/data/geo/events /user/dburkh/data/geo/events /user/dburkh/analytics/user_geo
    source_events_path   = sys.argv[1]   # исходные события
    target_events_path   = sys.argv[2]   # куда переложить с партицией
    output_vitrina_path  = sys.argv[3]   # путь для витрины
    
    geo_path = "/user/dburkh/data/geo/geo.csv"
    
    today = datetime.now().strftime("%Y-%m-%d")

    conf = SparkConf().setAppName(f"UserGeoVitrina-{today}")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    #----------------------------------------------------------------------------
    # 1. Чтение исходных событий + ОГРАНИЧЕНИЕ ДАННЫХ ДЛЯ ОТЛАДКИ
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

    # 1. Читаем без header — все данные в одной колонке
    # Читаем без header — Spark создаст колонку _c0
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

# Обязательно для проверки 
    print("Схема после парсинга geo.csv:")
    cities.printSchema()

    print("Первые 5 строк:")
    cities.show(5, truncate=False)
    #----------------------------------------------------------------------------
    # 4. Определение ближайшего города для каждого события
    #----------------------------------------------------------------------------
    cities_bc = F.broadcast(cities)

    with_distance = events.crossJoin(cities_bc) \
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
                                  "city",
                                  "city_id"
                              )

    #----------------------------------------------------------------------------
    # 5. act_city — город последнего события
    #----------------------------------------------------------------------------
    w_last = Window.partitionBy("user_id").orderBy(F.desc("event_time_utc"))

    act_city_df = geo_events.withColumn("rn", F.row_number().over(w_last)) \
                            .filter("rn = 1") \
                            .select(
                                "user_id",
                                F.col("city").alias("act_city")
                            )

    #----------------------------------------------------------------------------
    #6. local_time последнего события с учётом timezone
    #----------------------------------------------------------------------------
    last_event = geo_events.withColumn("rn", F.row_number().over(w_last)) \
                           .filter("rn = 1") \
                           .select("user_id", "event_time_utc", "city")

    last_with_tz = last_event.withColumn("timezone", get_timezone(F.col("city")))

    last_with_local = last_with_tz.withColumn(
        "local_time",
        F.from_utc_timestamp(F.col("event_time_utc"), F.col("timezone"))
    ).select("user_id", "local_time")

    #----------------------------------------------------------------------------
    #7.Итоговая витрина 
    #----------------------------------------------------------------------------
    vitrina = act_city_df.join(
        last_with_local, "user_id", "left_outer"
    ).withColumn("calc_date", F.lit(today))

    #Сохранение витрины
    vitrina.write \
           .mode("overwrite") \
           .partitionBy("calc_date") \
           .format("parquet") \
           .save(output_vitrina_path)

    sc.stop()


if __name__ == "__main__":
    main()
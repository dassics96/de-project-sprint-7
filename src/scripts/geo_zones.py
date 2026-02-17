import sys
from datetime import datetime

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.sql.functions import split, regexp_replace, col, trim

def haversine_expr(lat1, lon1, lat2, lon2):
    """Haversine в чистом Spark SQL"""
    lat1_rad = F.radians(lat1)
    lon1_rad = F.radians(lon1)
    lat2_rad = F.radians(lat2)
    lon2_rad = F.radians(lon2)

    a = (
        F.pow(F.sin((lat2_rad - lat1_rad) / 2), 2) +
        F.cos(lat1_rad) * F.cos(lat2_rad) *
        F.pow(F.sin((lon2_rad - lon1_rad) / 2), 2)
    )
    c = 2 * F.atan2(F.sqrt(a), F.sqrt(F.lit(1) - a))
    return F.lit(6371.0) * c


def main():
    # Аргументы:
    #1 — путь к событиям (уже партиционированным)
    #2 — путь к geo.csv
    #3 — выходной путь для витрины
    events_path     = sys.argv[1]   # /user/dburkh/data/geo/events
    geo_path        = sys.argv[2]   # /user/dburkh/data/geo/geo.csv
    output_path     = sys.argv[3]   # /user/dburkh/analytics/geo_zones
    
    geo_path = "/user/dburkh/data/geo/geo.csv"
    
    today = datetime.now().strftime("%Y-%m-%d")

    conf = SparkConf().setAppName(f"GeoZonesVitrina-{today}")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    #---------------------------------------------------------------
    # 1. Чтение + отладочный семпл
    #---------------------------------------------------------------
    events = sql.read.parquet(events_path)

    # Ограничение данных для отладки / проверки
    events = events.where("date >= '2022-04-01'") \
                   .sample(fraction=0.08, seed=20232023)

    print("INFO: using sampled data (date >= 2022-04-01 + 8% sample)")

    # user_id
    events = events.withColumn(
        "user_id",
        F.coalesce(
            F.col("event.message_from").cast("long"),
            F.col("event.reaction_from").cast("long"),
            F.col("event.subscription_user").cast("long"),
            F.col("event.user").cast("long")
        )
    ).filter(F.col("user_id").isNotNull())

    #---------------------------------------------------------------
    # 2. Последнее сообщение пользователя → источник координат
    #---------------------------------------------------------------
    messages = events.filter(F.col("event_type") == "message") \
                     .select(
                         "user_id",
                         "lat",
                         "lon",
                         "date",
                         F.to_timestamp(F.col("event.message_ts")).alias("event_time_utc")
                     )

    w_last = Window.partitionBy("user_id").orderBy(F.desc("event_time_utc"))

    last_msg_coords = messages.withColumn("rn", F.row_number().over(w_last)) \
                              .filter("rn = 1") \
                              .drop("rn") \
                              .select(
                                  "user_id",
                                  F.col("lat").alias("last_lat"),
                                  F.col("lon").alias("last_lon")
                              )

    #---------------------------------------------------------------
    # 3. Эффективные координаты для всех событий
    #---------------------------------------------------------------
    events_geo = events.join(last_msg_coords, "user_id", "left") \
        .withColumn("effective_lat", F.coalesce(F.col("lat"), F.col("last_lat"))) \
        .withColumn("effective_lon", F.coalesce(F.col("lon"), F.col("last_lon"))) \
        .filter(F.col("effective_lat").isNotNull() & F.col("effective_lon").isNotNull())

    #---------------------------------------------------------------
    # 4. Города + broadcast + haversine
    #---------------------------------------------------------------
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


    cities_bc = F.broadcast(cities)

    with_dist = events_geo.crossJoin(cities_bc) \
        .withColumn("distance_km", haversine_expr("effective_lat", "effective_lon", "city_lat", "city_lon"))

    w_nearest = Window.partitionBy("user_id", "event_time_utc").orderBy("distance_km")

    events_city = with_dist.withColumn("rn", F.row_number().over(w_nearest)) \
                           .filter("rn = 1") \
                           .drop("rn", "distance_km", "city_lat", "city_lon", "last_lat", "last_lon") \
                           .select(
                               "user_id",
                               "event_type",
                               "date",
                               "city_id",
                               F.date_format("date", "yyyy-MM").alias("month"),
                               F.weekofyear("date").alias("week")
                           )

    #---------------------------------------------------------------
    # 5. Первое появление пользователя (регистрация)
    #---------------------------------------------------------------
    first_appearance = events_city.groupBy("user_id") \
        .agg(F.min("date").alias("first_date"))

    first_with_city = first_appearance.join(
        events_city,
        (events_city.user_id == first_appearance.user_id) &
        (events_city.date == first_appearance.first_date),
        "inner"
    ).select(
        first_appearance.user_id,
        "first_date",
        "month",
        "week",
        "city_id"
    )

    #---------------------------------------------------------------
    # 6. Агрегация по НЕДЕЛЯМ
    #---------------------------------------------------------------
    weekly_pivot = events_city.groupBy("month", "week", "city_id") \
        .pivot("event_type", ["message", "reaction", "subscription"]) \
        .agg(F.count("*"))

    weekly_reg = first_with_city.groupBy("month", "week", "city_id") \
        .agg(F.countDistinct("user_id").alias("user"))

    weekly_df = weekly_pivot.join(
        weekly_reg,
        ["month", "week", "city_id"],
        "left_outer"
    ).select(
        F.col("month"),
        F.col("week"),
        F.col("city_id").alias("zone_id"),
        F.coalesce(F.col("message"), F.lit(0)).alias("week_message"),
        F.coalesce(F.col("reaction"), F.lit(0)).alias("week_reaction"),
        F.coalesce(F.col("subscription"), F.lit(0)).alias("week_subscription"),
        F.coalesce(F.col("user"), F.lit(0)).alias("week_user")
    )

    #---------------------------------------------------------------
    # 7. Агрегация по МЕСЯЦАМ (отдельно)
    #---------------------------------------------------------------
    monthly_pivot = events_city.groupBy("month", "city_id") \
        .pivot("event_type", ["message", "reaction", "subscription"]) \
        .agg(F.count("*"))

    monthly_reg = first_with_city.groupBy("month", "city_id") \
        .agg(F.countDistinct("user_id").alias("user"))

    monthly_df = monthly_pivot.join(
        monthly_reg,
        ["month", "city_id"],
        "left_outer"
    ).select(
        F.col("month"),
        F.col("city_id").alias("zone_id"),
        F.coalesce(F.col("message"), F.lit(0)).alias("month_message"),
        F.coalesce(F.col("reaction"), F.lit(0)).alias("month_reaction"),
        F.coalesce(F.col("subscription"), F.lit(0)).alias("month_subscription"),
        F.coalesce(F.col("user"), F.lit(0)).alias("month_user")
    )

    #---------------------------------------------------------------
    # 8. Сохранение двух отдельных витрин
    #---------------------------------------------------------------
    # weekly
    weekly_df.write \
             .mode("overwrite") \
             .partitionBy("month") \
             .format("parquet") \
             .save(f"{output_path}/weekly")

    # monthly
    monthly_df.write \
              .mode("overwrite") \
              .partitionBy("month") \
              .format("parquet") \
              .save(f"{output_path}/monthly")

    sc.stop()


if __name__ == "__main__":
    main()
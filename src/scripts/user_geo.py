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

    raw = sql.read \
    .option("header", "true") \
    .option("delimiter", ";") \
    .csv("/user/dburkh/data/geo/geo.csv")

    cities = raw.select(
    F.col("id").cast("string").alias("city_id"),
    F.col("city"),
    # Replace comma with dot and cast to double — in one step
    F.regexp_replace(F.col("lat"), ",", ".").cast("double").alias("city_lat"),
    F.regexp_replace(F.col("lng"), ",", ".").cast("double").alias("city_lon")
    )
    cities = cities.withColumnRenamed("lat", "orig_lat") \
                   .withColumnRenamed("lng", "orig_lng")

    cities = cities.select(
        F.col("city_id"),
        F.col("city"),
        F.col("city_lat"),
        F.col("city_lon")
    )
    
    #----------------------------------------------------------------------------
    #4. Определение ближайшего города для каждого события
    #----------------------------------------------------------------------------
    events_clean = events.drop("city", "city_id")  # если они там есть — удаляем

    cities_bc = F.broadcast(cities)

    with_distance = events_clean.crossJoin(cities_bc) \
    .withColumnRenamed("city", "lookup_city") \
    .withColumnRenamed("city_id", "lookup_city_id") \
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
        F.col("lookup_city").alias("city"),        # ТОЛЬКО из справочника!
        F.col("lookup_city_id").alias("city_id")   # ТОЛЬКО из справочника!
    )

    #----------------------------------------------------------------------------
    #5.Метрики путешествий, список без идущих подряд повторов
    #----------------------------------------------------------------------------
    # Окно для упорядочивания событий пользователя по времени
    w_travel = Window.partitionBy("user_id").orderBy("event_time_utc")

    #Добавляем предыдущий город и флаг изменения
    travel_with_prev = geo_events.withColumn(
    "prev_city", F.lag("city").over(w_travel)
    ).withColumn(
    "is_new_visit",
    F.when(
        (F.col("city") != F.col("prev_city")) | F.col("prev_city").isNull(),
        1
    ).otherwise(0)
    )

    #Оставляем только строки, где началось новое посещение (или первая строка)
    travel_visits = travel_with_prev.filter(F.col("is_new_visit") == 1)

    #Теперь агрегируем: считаем количество посещений и собираем список
    travel_metrics = travel_visits.groupBy("user_id").agg(
    F.count("*").alias("travel_count"),           # количество реальных посещений
    F.collect_list("city").alias("travel_array")  # список без идущих подряд дублей
    )
   #----------------------------------------------------------------------------
   #6.Последнее событие пользователя-город, локальное время, timezone
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
    
    #----------------------------------------------------------------------------
    #7. Домашний город — где пользователь непрерывно пребывал > 27 дней 
    #----------------------------------------------------------------------------
    # Шаг 1:Distinct даты по пользователю и городу 
    distinct_days = geo_events.select(
    "user_id",
    "city",
    F.to_date("event_time_utc").alias("stay_date")
    ).distinct()

    #Шаг 2:Упорядочивание по пользователю и дате
    w_stay = Window.partitionBy("user_id").orderBy("stay_date")

    #Шаг 3:Определяем смены города и предыдущую дату
    stays_with_lag = distinct_days.withColumn(
    "prev_city", F.lag("city").over(w_stay)
    ).withColumn(
    "prev_date", F.lag("stay_date").over(w_stay)
    ).withColumn(
    "is_new_stay",
    F.when(
        (F.col("city") != F.col("prev_city")) | F.col("prev_city").isNull() |
        (F.col("stay_date") != F.col("prev_date") + 1),  # не непрерывный день
        1
    ).otherwise(0)
    )

    #Шаг 4:Присваиваем ID группе непрерывного пребывания (cumulative sum)
    stays_grouped = stays_with_lag.withColumn(
    "stay_group",
    F.sum("is_new_stay").over(w_stay)
    )

    #Шаг 5:Агрегируем по группам — min/max дата, длительность в днях
    stay_periods = stays_grouped.groupBy(
    "user_id", "city", "stay_group"
    ).agg(
    F.min("stay_date").alias("start_date"),
    F.max("stay_date").alias("end_date"),
    (F.datediff(F.max("stay_date"), F.min("stay_date")) + 1).alias("duration_days")
    ).orderBy("user_id", "start_date")

    #Шаг 6: Фильтруем периоды > 27 дней
    long_stays = stay_periods.filter("duration_days > 27")

    #Шаг 7: Для каждого пользователя берём последний такой период (max end_date)
    w_home = Window.partitionBy("user_id").orderBy(F.desc("end_date"))

    home_city_df = long_stays.withColumn(
    "rn", F.row_number().over(w_home)
    ).filter("rn = 1") \
    .select(
     "user_id",
     F.col("city").alias("home_city")
    )

    # ----------------------------------------------------------------------------
    #8.Итоговая витрина—объединяем с метриками путешествий
    # ----------------------------------------------------------------------------
    vitrina = last_event_enriched \
        .join(travel_metrics, "user_id", "left_outer") \
        .join(home_city_df, "user_id", "left_outer") \
        .withColumn("calc_date", F.lit(today)) \
        .withColumn("travel_count", F.coalesce(F.col("travel_count"), F.lit(0))) \
        .withColumn("travel_array", F.coalesce(F.col("travel_array"), F.array())) \
        .withColumn("home_city", F.coalesce(F.col("home_city"), F.col("act_city"))) \
        .select(
        "user_id",
        "act_city",
        "local_time",
        "home_city",
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
import unittest

from src.aggregations import aggregate_rdd, aggregate_sql, compare
from src.config import spark_session
from src.preprocessing import screen


class CoreTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = spark_session("TLC - test minimi")
        cls.addClassCleanup(cls.spark.stop)

    def test_filtri_giorno_e_fascia(self):
        source = self.spark.createDataFrame([
            ("2025-07-01 00:00:00", "2025-07-01 02:00:00", 200.0, 0.0),
            ("2025-07-06 04:00:00", "2025-07-06 08:00:00", 4.0, 5.0),
            ("2025-07-31 23:59:59", "2025-08-01 00:00:00", 0.01, 8.0),
            ("2025-08-01 00:00:00", "2025-08-01 00:30:00", 2.0, 3.0),
            ("2025-07-01 08:00:00", "2025-07-01 08:00:00", 0.0, -2.0),
            (None, "2025-07-01 08:00:00", float("nan"), float("inf")),
            ("2025-07-01 12:00:00", "2025-07-01 16:00:01", 8.0, 4.0),
            ("2025-07-01 16:00:00", "2025-07-01 17:00:00", 201.0, 4.0),
        ], """
            tpep_pickup_datetime string, tpep_dropoff_datetime string,
            trip_distance double, fare_amount double
        """)
        clean, counts = screen(source, "yellow")
        self.assertEqual(
            (counts["original_count"], counts["valid_count"], counts["removed_unique"]),
            (8, 3, 5),
        )
        self.assertEqual(
            [counts[rule] for rule in (
                "pickup_outside_month", "null_timestamp", "duration_nonpositive",
                "duration_over_limit", "distance_invalid", "distance_over_limit",
                "speed_over_limit", "revenue_invalid",
            )],
            [1, 1, 1, 1, 2, 1, 1, 2],
        )
        self.assertEqual(
            [tuple(row) for row in clean.orderBy("pickup_datetime").select("weekday", "time_slot").collect()],
            [(2, "00-04"), (7, "04-08"), (4, "20-24")],
        )
        boundary = clean.where("revenue_usd = 0").first()
        self.assertEqual((boundary.distance_miles, boundary.speed_mph), (200.0, 100.0))

    def test_equivalenza_rdd_sql_valori_attesi(self):
        frame = self.spark.createDataFrame([
            ("yellow", 2, "00-04", 10.0, 2.0, 20.0),
            ("yellow", 2, "00-04", 12.5, 6.0, 60.0),
            ("green", 2, "00-04", 7.25, 3.0, 18.0),
            ("hvfhv", 7, "20-24", 15.0, 4.0, 24.0),
        ], """
            vehicle_type string, weekday int, time_slot string,
            revenue_usd double, distance_miles double, speed_mph double
        """)
        rdd, sql = aggregate_rdd(frame), aggregate_sql(frame)
        self.assertEqual(compare(rdd, sql)["verdict"], "PASS")
        self.assertEqual(compare(rdd[:-1], sql)["verdict"], "FAIL")
        expected = [
            ("green", 2, "00-04", 1, 7.25, 3.0, 18.0),
            ("hvfhv", 7, "20-24", 1, 15.0, 4.0, 24.0),
            ("yellow", 2, "00-04", 2, 22.5, 4.0, 40.0),
        ]
        self.assertEqual(list(map(tuple, rdd)), expected)
        self.assertEqual(list(map(tuple, sql)), expected)

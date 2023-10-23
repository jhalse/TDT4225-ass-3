import argparse
import itertools
from tabulate import tabulate
from DbConnector import DbConnector
import pandas as pd
import numpy as np

import pandas as pd
from tabulate import tabulate
from DbConnector import DbConnector
import pandas


class Queries:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def query_one(self, table_name):
        query = "SELECT COUNT(*) FROM %s"
        self.cursor.execute(query % table_name)
        result = self.cursor.fetchone()
        print(f"Entries in {table_name}: {result[0]}")

    def query_two(self):
        number_of_trackpoints_per_user = "SELECT Activity.user_id, " \
                                         "  COUNT(TrackPoint.id) AS Number_of_trackpoints " \
                                         "FROM Activity " \
                                         "INNER JOIN TrackPoint ON Activity.id = TrackPoint.activity_id " \
                                         "GROUP BY Activity.user_id"

        query = "SELECT AVG(Number_of_trackpoints), " \
                "       MAX(Number_of_trackpoints), " \
                "       MIN(Number_of_trackpoints) " \
                "FROM ( %s ) AS number_of_trackpoints_per_user_query"
        self.cursor.execute(query % number_of_trackpoints_per_user)
        result = self.cursor.fetchone()
        average, maximum, minimum = result
        print(f"Average: {average}, maximum: {maximum} and minimum: {minimum} trackpoints per user")

    def query_three(self):
        query = "SELECT Activity.user_id, " \
                "   COUNT(Activity.id) AS Number_of_activities  " \
                "FROM Activity " \
                "GROUP BY Activity.user_id " \
                "ORDER BY Number_of_activities DESC " \
                "LIMIT 15"
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(
            f"Top 15 users with the highest number of activities: \n{tabulate(result, ['User ID', 'Number of Activities'])}")

    def query_four(self):
        query = "SELECT DISTINCT user_id " \
                "FROM Activity " \
                "WHERE transportation_mode = 'bus'"
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(f"Users taken the bus:")
        for user in result:
            print(user[0])

    def query_five(self):
        query = "SELECT user_id, " \
                "   COUNT(DISTINCT transportation_mode) AS transport_count " \
                "FROM Activity " \
                "WHERE transportation_mode IS NOT NULL " \
                "GROUP BY user_id " \
                "ORDER BY transport_count DESC " \
                "LIMIT 10"
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(
            f"Top 10 users with different transportation modes: \n{tabulate(result, ['User ID', 'Number of transportation modes'])}")

    def query_six(self):
        query = "SELECT user_id, transportation_mode, start_date_time, end_date_time " \
                "FROM Activity " \
                "GROUP BY user_id, transportation_mode, start_date_time, end_date_time " \
                "HAVING COUNT(*) > 1;"
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(f"duplicates: \n{tabulate(result)} ")

    def query_seven(self):
        subquery = "select user_id, transportation_mode, start_date_time, end_date_time " \
                "from Activity " \
                "where DATE(end_date_time) > DATE(start_date_time)"
                                
        query_a = "select COUNT(DISTINCT user_id) AS user_count " \
                "from ( %s ) as subquery "
        self.cursor.execute(query_a % subquery)
        result_a = self.cursor.fetchall()

        print(f"Number of users who have activities spanning two dates: \n{tabulate(result_a)}")

        query_b = "SELECT user_id, transportation_mode, TIMESTAMPDIFF(SECOND, start_date_time, end_date_time) AS duration " \
                "FROM ( %s ) AS subquery "
        
        self.cursor.execute(query_b % subquery)
        result_b = self.cursor.fetchall()
        print(f"Duration of activities spanning two dates: \n{tabulate(result_b)}")

    # Assuming that the task wants us to find each single person who has been close to any other person both in time and space
    def query_eight(self):
        query = """SELECT Activity.user_id, TrackPoint.date_time, TrackPoint.lat, TrackPoint.lon 
                   FROM TrackPoint 
                   INNER JOIN Activity ON TrackPoint.activity_id = Activity.id 
                   ORDER BY TrackPoint.date_time 
                """
        
        df = pd.read_sql(query, self.db_connection)

        def haversine(lat1, lon1, lat2, lon2, radius=6371):
            """
            Calculate the distance between two points on a sphere using the Haversine formula.
            """
            dlat = np.radians(lat2 - lat1)
            dlon = np.radians(lon2 - lon1)
            a = np.sin(dlat / 2) ** 2 + np.cos(np.radians(lat1)) * np.cos(np.radians(lat2)) * np.sin(dlon / 2) ** 2
            c = 2 * np.arcsin(np.sqrt(a))
            return radius * c

        df['distance'] = haversine(df['lat'].shift(), df['lon'].shift(), df['lat'], df['lon'])
        df["time_diff"] = df.groupby("user_id")["date_time"].diff()

        m1 = df["time_diff"] <= pd.Timedelta(seconds=30)
        m3 = df["user_id"] != df["user_id"].shift()
        m2 = df["distance"] <= 50
        n_unique = df[m1 & m2 & m3]["user_id"].nunique()
        print(f"Number of unique user ids: {n_unique}")

    def query_nine(self):
        query_altitude_trackpoint = "SELECT Activity.user_id, activity_id, altitude " \
                                    "FROM TrackPoint " \
                                    "JOIN Activity ON Activity.id = TrackPoint.activity_id "
        df = pandas.read_sql(query_altitude_trackpoint, self.db_connection)
        # handle invalid values
        df = df[df['altitude'] != -777]
        # diff(): calculates the difference between current and prev row on altitude ie. altitude difference
        df['altitude_gain'] = df.groupby(['user_id', 'activity_id'])['altitude'].diff()
        # replace negative gains with 0
        df['altitude_gain'] = df['altitude_gain'].apply(lambda x: x if x > 0 else 0)
        # summing up altitude gains, now only positive values
        total_altitude_gain = df.groupby('user_id')['altitude_gain'].sum().reset_index()
        # sort users by gain
        sorted_users = total_altitude_gain.sort_values(by='altitude_gain', ascending=False)
        top_15_users = sorted_users.head(15)
        print(top_15_users)

    def query_10_longest_distances_per_transportation_mode_per_day(self):
        query = """
             SELECT 
                transportation_mode,
                user_id,
                MAX(total_distance) AS max_distance
            FROM (
                SELECT
                    a.user_id,
                    a.transportation_mode,
                    DATE(tp1.date_time) as travel_date,
                    SUM(
                        ST_DISTANCE_SPHERE(
                            POINT(tp1.lon, tp1.lat),
                            POINT(tp2.lon, tp2.lat)
                        ) / 1000 
                    ) AS total_distance
                FROM
                    Activity a
                JOIN 
                    TrackPoint tp1 ON a.id = tp1.activity_id
                JOIN
                    TrackPoint tp2 ON a.id = tp2.activity_id AND tp1.id = tp2.id - 1
                WHERE
                    a.transportation_mode IS NOT NULL
                GROUP BY 
                    a.user_id,
                    a.transportation_mode,
                    DATE(tp1.date_time)
            ) AS distances
            GROUP BY
                transportation_mode, user_id;
        """
        result = pd.read_sql_query(query, self.db_connection)
        result = result.sort_values('max_distance', ascending=False).drop_duplicates(['transportation_mode'])

        print(
            f"Users that have traveled the longest total distance in one day for each transportation mode: \n{tabulate(result, headers=['transportation_mode', 'user_id', 'distance (km)'])} ")

    def query_11_users_with_invalid_activities(self):
        query = """
            SELECT
                a.user_id,
                COUNT(DISTINCT a.id) as invalid_activities
            FROM
                Activity a
            JOIN TrackPoint t1 ON
                a.id = t1.activity_id
            JOIN TrackPoint t2 ON
                a.id = t2.activity_id
                AND t1.id = t2.id - 1
                AND TIMESTAMPDIFF(
                    MINUTE,
                    t1.date_time,
                    t2.date_time
                ) > 5
            GROUP BY
                a.user_id;
        """
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(f"All users with invalid activities: \n{tabulate(result, headers=['user_id', 'invalid_activities'])} ")

    def query_12_users_with_their_most_used_transportation_mode(self):
        query = """
            SELECT
                user_id,
                transportation_mode
            FROM
                (
                SELECT
                    user_id,
                    transportation_mode,
                    ROW_NUMBER() OVER(
                    PARTITION BY user_id
                ORDER BY
                    COUNT(*)
                DESC
                ) AS activity_rank
            FROM
                Activity
            WHERE
                transportation_mode IS NOT NULL
            GROUP BY
                user_id,
                transportation_mode) AS ranked_activities
                WHERE
                    activity_rank = 1
                GROUP BY
                    user_id,
                    transportation_mode
                ORDER BY
                    user_id;
        """
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(
            f"All users with registered transportation modes and their most used transportation mode: \n{tabulate(result, headers=['user_id', 'transportation_mode'])} ")


def main():
    program = None
    try:
        program = Queries()
        table_names = ["Activity", "TrackPoint", "User"]
        # cleanly run queries based on argument
        if query == 1:
            for name in table_names:
                program.query_one(table_name=name)
        elif query == 2:
            program.query_two()
        elif query == 3:
            program.query_three()
        elif query == 4:
            program.query_four()
        elif query == 5:
            program.query_five()
        elif query == 6:
            program.query_six()
        elif query == 7:
            program.query_seven()
        elif query == 8:
            program.query_eight()
        elif query == 9:
            program.query_nine()
        elif query == 10:
            program.query_10_longest_distances_per_transportation_mode_per_day()
        elif query == 11:
            program.query_11_users_with_invalid_activities()
        elif query == 12:
            program.query_12_users_with_their_most_used_transportation_mode()
        else:
            print("ERROR: Invalid query number")

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    # Use args to be able to choose which query you want to run
    parser = argparse.ArgumentParser(description="Choose query")
    parser.add_argument("-query", type=int, help="Choose query")
    args = parser.parse_args()
    main(args.query)
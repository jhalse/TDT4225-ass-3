import argparse
import itertools
from pprint import pprint

from tabulate import tabulate
from DbConnector import DbConnector
import pandas as pd
import numpy as np

import pandas as pd
from tabulate import tabulate
from DbConnector import DbConnector
import pandas


class Queries:

    def __init__(self, db_connector):
        self.connection = db_connector
        self.client = db_connector.client
        self.db = db_connector.db
        self.users_collection = self.db["users"]
        self.activities_collection = self.db["activities"]
        self.trackpoints_collection = self.db["trackpoints"]

    """How many users, activities and trackpoints are there in the dataset"""
    def query_one(self):
        user_count = self.users_collection.count_documents({})
        activity_count = self.activities_collection.count_documents({})
        trackpoint_count = self.trackpoints_collection.count_documents({})

        table_data = [
        ["Users", user_count],
        ["Activities", activity_count],
        ["Trackpoints", trackpoint_count]]

        print(tabulate(table_data, headers=["Collection", "Count"]))
        

    def query_two(self):
        result = self.activities_collection.aggregate([
            {"$group": {"_id": "$user_id", "count": {"$sum": 1}}},
            {"$group": {"_id": "null", "avg": {"$avg": "$count"}}}
        ])
        average_activities_per_user = list(result)[0]['avg']
        print(f"Average number of activities per user: {average_activities_per_user :.2f}")

    """Find the top 20 users with the highest number of activities."""
    def query_three(self):
        group_users = {"$group": {"_id": "$user_id", "count": {"$sum": 1}}}
        sort_by_count = {"$sort": {"count": -1}}
        limit = {"$limit": 20}

        top_users = self.activities_collection.aggregate([group_users, sort_by_count, limit])

        table = [(user["_id"], user["count"]) for user in top_users]
        print(tabulate(table, headers=["User", "Number of activities"]))
        

    def query_four(self):
        result = self.activities_collection.find({"transportation_mode": "taxi"}, {"user_id": 1, "_id": 0})
        print("All users who have taken a taxi:")
        pprint(list(result))

    def query_five(self):
        transportation_not_null = { "$match": {"transportation_mode": {"$ne": None}}}
        group_transportation = {"$group": { "_id": "$transportation_mode", "activity_count": {"$sum": 1}}}
        sort_by_count = {"$sort": {"activity_count": -1}}

        transportation_modes_in_activites = self.activities_collection.aggregate([transportation_not_null, group_transportation, sort_by_count])
        table = [(transport["_id"], transport["activity_count"]) for transport in transportation_modes_in_activites]
        print(tabulate(table, headers=["Transportation mode", "Number of activities"]))

    def query_six(self):
        # result = self.activities_collection.find_one()
        # pprint(result)

        # a)
        result = self.activities_collection.aggregate([
            {"$group": {"_id": {"$year": "$start_date_time"}, "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 1}
        ])
        result = list(result)
        year_with_most_activities = result[0]['_id']
        year_with_most_activities_count = result[0]['count']

        # b)
        result = self.activities_collection.aggregate([
            {
                "$project":
                    {
                        "year": {"$year": "$start_date_time"},
                        "duration": {
                            "$divide": [
                                {
                                    "$subtract": ["$end_date_time", "$start_date_time"]}, 3600000  # ms to hours
                            ]
                        }
                    }
            },
            {"$group": {"_id": "$year", "total_hours": {"$sum": "$duration"}}},
            {"$sort": {"total_hours": -1}},
            {"$limit": 1}
        ])

        result = list(result)
        year_with_most_hours = result[0]['_id']
        year_with_most_hours_count = result[0]['total_hours']
        is_same_year = year_with_most_hours == year_with_most_activities

        print(f"Year with most activities: {year_with_most_activities} ({year_with_most_activities_count})")
        print(f"Year with most hours: {year_with_most_hours} ({year_with_most_hours_count:.2f})")
        print(f"Is the year with most activities the same as the year with most hours? {is_same_year}")

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
        # Find the top 20 users who have gained the most altitude where altidude is not -777
        result = self.trackpoints_collection.aggregate([
            {"$match": {"altitude": {"$ne": -777, "$exists": True}}},
            {"$group": {"_id": "$user_id", "altitude_diff": {"$sum": "$altitude"}}},  # TODO: need user id in trackpoint, use altidude_diff in insertion?
            {"$sort": {"altitude_diff": -1}},
            {"$limit": 20}
        ])
        result = list(result)
        print("Top 20 users who have gained the most altitude")
        pprint(list(result))

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


def main(query):
    program = None
    try:
        db_connector = DbConnector(DATABASE="my_db", HOST="tdt4225-21.idi.ntnu.no", USER="mongo", PASSWORD="mongo")

        program = Queries(db_connector)

        # cleanly run queries based on argument
        if query == 1:
            pass
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

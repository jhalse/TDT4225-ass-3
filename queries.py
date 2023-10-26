import argparse
from datetime import datetime
from pprint import pprint

import pandas as pd
from haversine import haversine
from tabulate import tabulate

from DbConnector import DbConnector


class Queries:

    def __init__(self, db_connector):
        self.connection = db_connector
        self.client = db_connector.client
        self.db = db_connector.db
        self.users_collection = self.db["users"]
        self.activities_collection = self.db["activities"]
        self.trackpoints_collection = self.db["trackpoints"]

    def query_one(self):
        """How many users, activities and trackpoints are there in the dataset"""
        user_count = self.users_collection.count_documents({})
        activity_count = self.activities_collection.count_documents({})
        trackpoint_count = self.trackpoints_collection.count_documents({})

        table_data = [
        ["Users", user_count],
        ["Activities", activity_count],
        ["Trackpoints", trackpoint_count]]

        print(tabulate(table_data, headers=["Collection", "Count"]))
        

    def query_two(self):
        """Find the average number of activities per user.

        Calculates the number of activities for each user and then calculates the average.
        """
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
        """Find all users who have taken a taxi."""
        result = self.activities_collection.distinct(
            "user_id",
            {
                "transportation_mode": "taxi"
            }
        )

        print(tabulate([[line] for line in list(result)], headers=["All users who have taken a taxi"]))

    def query_five(self):
        transportation_not_null = { "$match": {"transportation_mode": {"$ne": None}}}
        group_transportation = {"$group": { "_id": "$transportation_mode", "activity_count": {"$sum": 1}}}
        sort_by_count = {"$sort": {"activity_count": -1}}

        transportation_modes_in_activites = self.activities_collection.aggregate([transportation_not_null, group_transportation, sort_by_count])
        table = [(transport["_id"], transport["activity_count"]) for transport in transportation_modes_in_activites]
        print(tabulate(table, headers=["Transportation mode", "Number of activities"]))

    def query_six(self):
        """
        For the grouping by year we only consider the start_date_time of activities.

        a) Find the year with the most activities.
        We extract the year from the start_date_time and group by year, yielding the count for each year.

        b) Is this also the year with most recorded hours?
        We extract the year from the start_date_time and calculate the duration of each activity.
        The duration is calculated by subtracting the start_date_time from the end_date_time and converting ms to hours
        This is summed for each year.
        """

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

    """Find the total distance (in km) walked in 2008, by user with id=112."""
    def query_seven(self):
        filter = { 
            "$match": {
                "user_id": {"$eq": "112"}, 
                "transportation_mode": {"$eq": "walk"}, 
                "start_date_time": {"$gte": datetime(2008, 1, 1), "$lt": datetime(2009, 1, 1)},
                "end_date_time": {"$gte": datetime(2008, 1, 1), "$lt": datetime(2009, 1, 1)}
            } 
        }

        activities = self.activities_collection.aggregate([filter])
        total_distance = 0

        for activity in activities:
            trackpoints = list(self.trackpoints_collection.find({"activity_id": activity['_id']}))
            
            for i in range(1, len(trackpoints)):
                trackpoint1 = (trackpoints[i-1]['lat'], trackpoints[i-1]['lon'])
                trackpoint2 = (trackpoints[i]['lat'], trackpoints[i]['lon'])
                total_distance += haversine(trackpoint1, trackpoint2)
        
        print(f"Total distance walked by user 112 in 2008: {total_distance} km")    
 

    def query_eight(self):
        """
        Find the top 20 users who have gained the most altitude where altidude is not -777.

        Activities are grouped by user id and the altitude difference is summed up for each user.
        The altitude difference is multiplied by 0.3048 to convert from feet to meters.
        """

        result = self.activities_collection.aggregate([
            {"$group": {"_id": "$user_id", "max_altitude_gain": {"$sum": {"$multiply": ["$altitude_diff", 0.3048]}}}},
            {"$sort": {"max_altitude_gain": -1}},
            {"$limit": 20}
        ])
        result = list(result)
        print("Top 20 users who have gained the most altitude")
        table = [(line["_id"], round(line["max_altitude_gain"], 4)) for line in result]
        print(tabulate(table, headers=["User", "Altitude gain"]))

    def query_nine(self):
        data = list(self.trackpoints_collection.find())
        df = pd.DataFrame(data)

        df["time_diff"] = df.groupby("activity_id")["date_from"].diff()

        df["invalid"] = df["time_diff"] > pd.Timedelta(minutes=5)

        result = df[df['invalid']].groupby('user_id')['activity_id'].nunique().reset_index()
        result.columns = ["UserId", "InvalidActivities"]
        pd.set_option('display.max_rows', None)
        print(result)
    

    def query_ten(self):
        """
        Find the users who have tracked an activity in the Forbidden City of Beijing.

        For this task we assume that the Forbidden City is the area between the following coordinates,
        where the area is defined by higher precision coordinates: lat 39.916, lon 116.397.
        """

        result = self.trackpoints_collection.aggregate(
            [
                {
                    "$match": {
                        "$expr": {
                            "$and": [
                                {"$gte": ["$lat", 39.916]},
                                {"$lt": ["$lat", 39.917]},
                                {"$gte": ["$lon", 116.397]},
                                {"$lt": ["$lon", 116.398]},
                            ]
                        }
                    }
                },
                {"$group": {"_id": "$user_id"}},
            ]
        )

        result = list(result)

        print(tabulate([[line["_id"]] for line in result], headers=["Users"]))

    def query_eleven(self):
        """
        Find all users who have registered transportation_mode and their most used transportation_mode.

        First filter out all activities where transportation_mode is None.
        Then group by user_id and transportation_mode and count the number of activities for each user and transportation_mode.
        Then group by user_id and find the transportation_mode with the highest count for each user.
        """
        result = self.activities_collection.aggregate([
            {
                "$match": {
                    "transportation_mode": {
                        "$ne": None
                    }
                }
            },
            {
                "$group": {
                    "_id": {
                        "user_id": "$user_id",
                        "transportation_mode": "$transportation_mode"
                    },
                    "count_activities": {
                        "$count": {}
                    }
                }
            },
            {
                "$group": {
                    "_id": "$_id.user_id",
                    "most_used_transportation_mode": {
                        "$max": {
                            "max": "$count_activities",
                            "mode": "$_id.transportation_mode",
                        }
                    },
                }
            },
            {
                "$sort": {
                    "_id": 1
                }
            },
        ])

        result = list(result)
        table = [(line["_id"], line["most_used_transportation_mode"]["mode"], line["most_used_transportation_mode"]["max"]) for line in result]

        print("Users who have registered transportation_mode and their most used transportation_mode")
        print(tabulate(table, headers=["User", "Mode", "Count"]))

    def query_print_samples(self):
        user = self.users_collection.find_one()
        print("\nInstance of User:")
        pprint(user)

        activity = self.activities_collection.find_one({"transportation_mode": {"$ne": None}})
        print("\nInstance of Activity")
        pprint(activity)

        trackpoint = self.trackpoints_collection.find_one({"activity_id": activity["_id"]})
        print("\nInstance of Trackpoint")
        pprint(trackpoint)

def main(query):
    program = None
    try:
        db_connector = DbConnector(DATABASE="my_db", HOST="tdt4225-21.idi.ntnu.no", USER="mongo", PASSWORD="mongo")

        program = Queries(db_connector)

        # cleanly run queries based on argument
        if query == 1:
            program.query_one()
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
            program.query_ten()
        elif query == 11:
            program.query_eleven()
        elif query == 13:
            program.query_print_samples()
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

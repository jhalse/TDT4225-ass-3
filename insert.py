import os
import pandas as pd
from pymongo import MongoClient


class DataLoader:

    def __init__(self, data_dir="./dataset"):
        self.client = MongoClient('localhost', 27017)
        self.db = self.client['my_db']
        self.users_collection = self.db['users']
        self.activities_collection = self.db['activities']
        self.trackpoints_collection = self.db['trackpoints']
        self.data_dir = data_dir
        self.MAX_TRACK_POINTS_PER_ACTIVITY = 2500

    def load_users(self):
        user_records = []

        with open(self.data_dir + "/labeled_ids.txt", "r") as labeled_ids:
            user_ids_with_labels = [line.strip() for line in labeled_ids]

        for user_id in os.listdir(self.data_dir + "/Data"):
            has_labels = user_id in user_ids_with_labels
            user_records.append({'user_id': user_id, 'has_labels': has_labels})

        self.users_collection.insert_many(user_records)
        print(f"{len(user_records)} Records inserted successfully into User collection")

    def get_timestamps(self, df):
        start_date = df.iloc[0, 5]
        start_time = df.iloc[0, 6]
        start_date_time = start_date + " " + start_time
        end_date = df.iloc[-1, 5]
        end_time = df.iloc[-1, 6]
        end_date_time = end_date + " " + end_time
        return start_date_time, end_date_time

    def load_activities(self):
        data_dir = self.data_dir + "/Data"

        for user_id in os.listdir(data_dir):
            user_dir = data_dir + "/" + user_id
            labels = {}

            if "labels.txt" in os.listdir(user_dir):
                with open(user_dir + "/labels.txt", "r") as label_file:
                    lines = label_file.readlines()
                    for line in lines[1:]:
                        start, end, label = line.strip().split("\t")
                        start, end = start.replace("/", "-"), end.replace("/", "-")
                        labels[(start, end)] = label

            for activity in os.listdir(user_dir + "/Trajectory"):
                track_points = pd.read_csv(user_dir + "/Trajectory/" + activity, skiprows=6, header=None)

                if len(track_points) > self.MAX_TRACK_POINTS_PER_ACTIVITY:
                    continue

                start_date_time, end_date_time = self.get_timestamps(track_points)
                transportation_mode = labels.get((start_date_time, end_date_time), None)

                track_points_list = track_points.apply(
                    lambda row: {"activity_id": activity, 'lat': row[0], 'lon': row[1], 'altitude': row[3], 'date_days': row[4],
                                 'date_time': row[5] + " " + row[6]}, axis=1).tolist()

                activity_record = {
                    "user_id": user_id,
                    'transportation_mode': transportation_mode,
                    'start_date_time': start_date_time,
                    'end_date_time': end_date_time
                }

                activity_id = self.activities_collection.insert_one(activity_record).inserted_id

                for tp in track_points_list:
                    tp["activity_id"] = activity_id

                self.trackpoints_collection.insert_many(track_points_list)
                print(f"{len(track_points_list)} trackpoints inserted successfully for activity {activity}")

            print(f"Activities and trackpoints inserted successfully for user {user_id}")

        print("All records inserted successfully.")


if __name__ == "__main__":
    loader = DataLoader()
    loader.load_users()
    loader.load_activities()

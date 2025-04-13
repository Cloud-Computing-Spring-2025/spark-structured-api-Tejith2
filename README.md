📄 README.md – Spark Structured API: Music Listening Behavior Analysis
markdown
Copy
Edit
# 🎵 Spark Structured API – Music Listening Behavior Analysis

## 📘 Overview
This project uses **Apache Spark Structured APIs** to analyze music streaming behavior from a fictional platform. The analysis focuses on user listening patterns, song popularity, genre trends, and engagement metrics.

Two datasets were generated:
- `listening_logs.csv`: Simulates user listening history.
- `songs_metadata.csv`: Contains details about songs like genre, mood, and artist.

---

## 📁 Dataset Descriptions

### 🎧 listening_logs.csv
| Column        | Description                               |
|---------------|-------------------------------------------|
| user_id       | Unique identifier of a user               |
| song_id       | Unique identifier of a song               |
| timestamp     | Datetime when the user played the song    |
| duration_sec  | Duration of the play in seconds           |

### 🎼 songs_metadata.csv
| Column   | Description                                |
|----------|--------------------------------------------|
| song_id  | Unique identifier of a song                |
| title    | Title of the song                          |
| artist   | Artist name                                |
| genre    | Genre (e.g., Pop, Rock, Jazz, etc.)        |
| mood     | Mood category (e.g., Happy, Sad, Chill)    |

---

## ✅ Tasks Completed & Output Structure

| Task No. | Task Description                                                                          | Output Folder                      |
|---------:|--------------------------------------------------------------------------------------------|------------------------------------|
| Task 1   | Find each user's favorite genre                                                           | `output/user_favorite_genres/`     |
| Task 2   | Calculate average listen time per song                                                    | `output/avg_listen_time_per_song/` |
| Task 3   | List top 10 most played songs this week                                                   | `output/top_songs_this_week/`      |
| Task 4   | Recommend up to 3 "Happy" songs to users who mostly listen to "Sad" songs                | `output/happy_recommendations/`    |
| Task 5   | Compute genre loyalty score for each user (loyalty > 0.5 shown)                           | `output/genre_loyalty_scores/`     |
| Task 6   | Identify users who listen to music between 12 AM and 5 AM                                | `output/night_owl_users/`          |

---

## 🗃️ Folder Structure

spark-structured-api-Tejith2/ ├── generate_listening_logs.py ├── generate_songs_metadata.py ├── task2_avg_listen_time.py ├── task3_top_songs_this_week.py ├── task4_happy_recommendations.py ├── task5_genre_loyalty.py ├── task6_night_owl_users.py ├── listening_logs.csv ├── songs_metadata.csv ├── output/ │ ├── user_favorite_genres/ │ ├── avg_listen_time_per_song/ │ ├── top_songs_this_week/ │ ├── happy_recommendations/ │ ├── genre_loyalty_scores/ │ ├── night_owl_users/ └── README.md

yaml
Copy
Edit

---

## ▶️ How to Run

### Generate Datasets:
```bash
python3 generate_songs_metadata.py
python3 generate_listening_logs.py
Execute Each Task:
bash
Copy
Edit
spark-submit task2_avg_listen_time.py

u1,Pop,20
u10,Pop,13
u11,Classical,14
u12,Pop,17
u13,Classical,13
u14,Classical,11
u15,Rock,20
u16,Pop,11
u17,Hip-Hop,11
u18,Rock,19
u19,Hip-Hop,12
u20,Hip-Hop,15

spark-submit task3_top_songs_this_week.py
s21,160.5
s25,166.58
s27,179.5
s55,198.81
s60,142.86
s75,174.91
s82,173.57
s85,188.57
s96,197.38

spark-submit task4_happy_recommendations.py

s21,6
s8,5
s27,5
s82,5
s73,5
s94,5
s3,5
s85,5
s75,4
s63,4

spark-submit task5_genre_loyalty.py

u16,s53
u16,s50
u16,s55
u19,s50
u19,s55
u20,s53
u20,s22
u20,s96
u3,s22
u3,s96
u3,s56
u4,s50
u4,s55

spark-submit task6_night_owl_users.py

u1,Pop,0.3482
u10,Pop,0.3023
u11,Classical,0.2641
u12,Pop,0.3617
u15,Rock,0.3508
u18,Rock,0.3653
u20,Hip-Hop,0.2830
u3,Classical,0.2352
u4,Rock,0.2541


🐞 Errors & Resolutions
Issue	                         Fix

Window is not defined	Added from pyspark.sql.window import Window
Empty outputs	Lowered thresholds (e.g., sad ratio > 0.3, loyalty > 0.5) to test
CSV file not found	Corrected path from subfolder to project roo

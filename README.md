# Music Streaming Analysis Using Spark Structured APIs

## Overview
This project analyzes music streaming data using PySpark Structured APIs. It processes listening logs and song metadata to derive insights about user preferences, listening habits, and genre loyalty. The analysis demonstrates key Spark concepts including DataFrames, window functions, aggregations, and transformations.

## Dataset Description
**listening_logs.csv**

Contains user listening history with the following fields:

user_id: Unique identifier for each user (user_1 to user_100)

song_id: Unique identifier for each song (song_1 to song_50)

timestamp: Date and time of listening event (March 2025)

duration_sec: Duration of listening in seconds (30-300 seconds)

**songs_metadata.csv**

Contains song information with the following fields:

song_id: Unique identifier for each song (matches listening_logs)

title: Song title

artist: Artist name (artist_1 to artist_20)

genre: Music genre (Pop, Rock, Jazz, Classical, Hip-Hop)

mood: Song mood (Happy, Sad, Energetic, Chill)

## Repository Structure
Hands-on-L6-main/

├── datagen.py                 # Data generation script

├── main.py                    # Main Spark analysis script

├── README.md                  # Project documentation

├── Requirements               # Dependencies list

├── listening_logs.csv         # Generated listening data

├── songs_metadata.csv         # Generated song metadata

└── outputs/                   # Analysis results directory

      ├── user_favorite_genres/      # Task 1 results
    
      ├── avg_listen_per_user/       # Task 2 results (users)
    
      ├── avg_listen_per_song/       # Task 2 results (songs)
    
      ├── top_10_loyalty_scores/     # Task 3 results
    
      └── night_listeners/           # Task 4 results

## Output Directory Structure
outputs/

├── user_favorite_genres/

│   ├── part-00000-xxx.csv    # User favorite genres

│   └── _SUCCESS

├── avg_listen_per_user/

│   ├── part-00000-xxx.csv    # Average listen time per user

│   └── _SUCCESS

├── avg_listen_per_song/

│   ├── part-00000-xxx.csv    # Average listen time per song

│   └── _SUCCESS

├── top_10_loyalty_scores/

│   ├── part-00000-xxx.csv    # Top 10 user-genre loyalty scores

│   └── _SUCCESS

└── night_listeners/

   ├── part-00000-xxx.csv    # Users who listen between 12 AM-5 AM
   
   └── _SUCCESS

## Tasks and Outputs
**Task 1: User Favorite Genres**

Description: For each user, determine their most-listened genre based on total listening time.

Logic:
Join listening logs with song metadata

Aggregate total listening time per user per genre

Use window functions to rank genres per user

Select the top-ranked genre for each user

Output: user_favorite_genres/ - Contains user_id, genre, total_listen_time

**Task 2: Average Listen Time**

Description: Calculate average listening duration per user and per song.

Logic:
Group by user_id and compute average duration_sec

Group by song_id and compute average duration_sec

Calculate overall average for reference

Output:

avg_listen_per_user/ - Contains user_id, avg_listen_duration

avg_listen_per_song/ - Contains song_id, avg_listen_duration

**Task 3: Genre Loyalty Scores**

Description: Create a custom loyalty score for each user-genre combination and rank the top 10.

Logic:
Calculate listen count and total time per user-genre

Calculate total listening time per user

Create loyalty score formula:

loyalty_score = listen_count * (total_time / user_total_time)

Sort by loyalty score descending and take top 10

Output: top_10_loyalty_scores/ - Contains user_id, genre, loyalty_score

**Task 4: Night Owl Users**

Description: Identify users who listen to music between 12 AM and 5 AM.

Logic:
Extract hour from timestamp

Filter records where hour is between 0 and 5

Select distinct user_ids

Output: night_listeners/ - Contains user_id of night listeners

## Execution Instructions
## *Prerequisites*

Before starting the assignment, ensure you have the following software installed and properly configured on your machine:

1. *Python 3.x*:
   - [Download and Install Python](https://www.python.org/downloads/)
   - Verify installation:
     ```bash
     python3 --version
     ```

2. *PySpark*:
   - Install using pip:
     ```bash
     pip install pyspark
     ```

3. *Apache Spark*:
   - Ensure Spark is installed. You can download it from the [Apache Spark Downloads](https://spark.apache.org/downloads.html) page.
   - Verify installation by running:
     ```bash
     spark-submit --version
     ```

### *2. Running the Analysis Tasks*

####  *Running Locally*

1. *Generate the Input*:
  ```bash
   python3 input_generator.py
   ```

2. **Execute Each Task Using spark-submit**:
   ```bash
     spark-submit main.py
   ```

3. *Verify the Outputs*:
   Check the outputs/ directory for the resulting files:
   ```bash
   ls outputs/
   ```

## Errors and Resolutions

| Error | Cause | Resolution |
|-------|-------|------------|
| `command not found: pip` | pip not in PATH | Use `python3 -m pip install pyspark` |
| `getSubject is not supported` | Java 24+ incompatible with Spark | Set Java 17: `export JAVA_HOME=$(/usr/libexec/java_home -v 17)` |
| `NameError: name 'user_favorite_genge' is not defined` | Typo in main.py | Change `user_favorite_genge.show()` to `user_favorite_genre.show()` |
| `No such file or directory: 'input_generator.py'` | Wrong filename | Use `python3 datagen.py` instead |
| `No such file or directory: 'listening_logs.csv'` | Data not generated | Run `python3 datagen.py` first |
| `ModuleNotFoundError: No module named 'pyspark'` | PySpark not installed | Run `python3 -m pip install pyspark` |
| `Unable to load native-hadoop library` | Missing Hadoop libraries | Warning only - can be ignored |
| `Outputs directory not created` | Script failed | Check error messages and fix underlying issue |

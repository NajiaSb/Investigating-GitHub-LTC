""" This code collects data for the Developer Profile feature table.
"""
from dotenv import load_dotenv
import polars as pl
import pandas as pd
import requests
import os
import re
import json
import time
from concurrent.futures import ThreadPoolExecutor

load_dotenv()

GITHUB_TOKEN = os.getenv("GIT_TOKEN3")
MAX_GHTORRENT_DATE = pd.to_datetime("2021-03-06 23:57:37+00:00")  # Max date from GHTorrent data
HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}"
}

def check_rate_limit():
    response = requests.get("https://api.github.com/rate_limit", headers=HEADERS)
    if response.status_code == 200:
        rate_limit = response.json()
        remaining = rate_limit["rate"]["remaining"]
        reset_time = rate_limit["rate"]["reset"]
        # print(f"\t\tRemaining Requests: {remaining}, Reset Time: {time.ctime(reset_time)}")
        return remaining, reset_time
    return 0, 0

def get_github_data(url):
    url = re.sub(r"\{.*?\}", "", url)  # Clean URL placeholders
    collected_data = []
    params = {"per_page":100}
    while url:
        remaining, reset_time = check_rate_limit()
        if remaining == 0:
            wait_time = max(1, reset_time - time.time())
            print(f"\tRate limit exceeded! Waiting {wait_time:.2f} seconds before retrying...")
            time.sleep(wait_time + 5)
            continue
        
        response = requests.get(url, headers=HEADERS, params=params)    
        if response.status_code != 200:
            break
        data = response.json()
        collected_data.extend(data)
        url = response.links.get('next', {}).get('url')
    return collected_data

def get_own_count(registration_date, url):
    """
    Function that returns the total of items
    filtered registration_date in any passed url
    with date data
    """
    dataset = get_github_data(url)
    if not dataset:  # Check if repos is None or empty
        return 0  # Return 0 instead of printing
    total = []
    for data in dataset:
        creation_date = data["created_at"] if data["created_at"] else None    
        if pd.to_datetime(creation_date) <= registration_date:
            total.append(data) 
    return len(total)  # Return the count for further use

def get_watch_count(user_id, registration_date, watchers):
    watches_filtered = watchers.filter(
        (pl.col("user_id") == user_id) & 
        (pl.col("created_at") <= registration_date)        
    )
    return watches_filtered.height

def get_pull_and_issues(user_id, registration_date, issues):
    issues_filtered = issues.filter(
        (pl.col("reporter_id") == user_id) & 
        (pl.col("created_at") <= registration_date) &
        (pl.col("pull_request") <= 0)       
    )
    pulls_filtered = issues.filter(
        (pl.col("reporter_id") == user_id) & 
        (pl.col("created_at") <= registration_date) &     
        (pl.col("pull_request") <= 1)
    )
    return issues_filtered.height, pulls_filtered.height

def get_followers_count(user_id, registration_date, followers):

    followers_filtered = followers.filter(
        (pl.col("user_id") == user_id) & 
        (pl.col("created_at") <= registration_date)        
    )
    return followers_filtered.height

def count_commits(user_id, registration_date, commits):

    commits_filtered = commits.filter(
        (pl.col("author_id") == user_id) & 
        (pl.col("created_at") <= registration_date)     
    )
   
    # user_history_commits += commits_filtered.height
    
    # # Count number of unique projects (repositories)
    # user_contribute_repos += commits_filtered.select(pl.col("project_id").n_unique()).item()

    return commits_filtered.select(pl.col("project_id").n_unique()).item(), commits_filtered.height
    
def create_developer_profile(repo_num, repo_data, contributor_list, csv_path, watchers, issues, followers, commits):
    """
    Function that creates or appends developer profile data to a CSV file.
    """
    repo_id = repo_data["id"]
    repo_name = repo_data["name"]

    # Define DataFrame columns
    columns = [
        "repo_name", "repo_id", "user_id", "user_age", "registration_date", "user_own_repos",
        "user_watch_repos", 
        "user_contribute_repos",  "user_history_commits",
        "user_history_pull_requests", "user_history_issues", "user_history_followers", 
        "ltc_1", "ltc_2", "ltc_3"
    ]

    # Check if the CSV file already exists
    file_exists = os.path.isfile(csv_path)
    # Load existing data if the file exists
    if file_exists:
        existing_df = pd.read_csv(csv_path)
    else:
        existing_df = pd.DataFrame(columns=columns)
    new_data = []  # Store new rows before appending

    for i, contributor in enumerate(contributor_list):
        username = contributor["login"]
        user_id = contributor["id"]
        registration_date = pd.to_datetime(contributor["registration_date"])
    
        # Check if user already exists in the CSV
        if file_exists and user_id in existing_df["user_id"].values:
            print(f"{repo_num} {repo_name} - User {username} already exists in dataset, skipping...")
            continue
        elif registration_date >= MAX_GHTORRENT_DATE:
            print(f"{repo_num}: {repo_name} - User {username} joined in {registration_date}, skipping...")
            continue
        else:
            print(f"{repo_num} {repo_name} - Processing {i}: {username} out of {len(contributor_list)}")

        ltc_1 = 1 if contributor["one_year"] == "yes" else 0
        ltc_2 = 1 if contributor["one_year"] == "yes" and contributor["two_years"] == "yes" else 0
        ltc_3 = 1 if contributor["LTC"] == "yes" else 0

        # FEATURE 1 - days between registration and joining the repo
        user_age = contributor["user_age"]
        
        # FEATURE 2 - number of repos the user owns before joining the repo
        user_own_repos = get_own_count(registration_date, contributor["repos_url"])
        
        # FEATURE 3 - number of repos a user watches
        user_watch_repos = get_watch_count(user_id, registration_date, watchers)
    
        # FEATURE 4 and 5
        user_contribute_repos, user_history_commits = count_commits(user_id, registration_date, commits)
        # FEATURE 6 and 7
        user_history_pull_requests, user_history_issues = get_pull_and_issues(user_id, registration_date, issues)
        # FEATURE 8
        user_history_followers =  get_followers_count(user_id, registration_date, followers)

        # Append new row to list
        new_data.append([
            repo_name, repo_id, user_id, user_age, registration_date.date(),
            user_own_repos, 
            user_watch_repos, 
            user_contribute_repos, user_history_commits, 
            user_history_pull_requests, user_history_issues, user_history_followers, 
            ltc_1, ltc_2, ltc_3
        ])
        # Save to csv
        temp_df = pd.DataFrame(new_data, columns=columns)
        temp_df.to_csv(csv_path, mode="a", header=not file_exists, index=False)
        file_exists = True  # Ensure header isn't written again after first write
        new_data = []  # Clear new data list

    print(f"Data saved to {csv_path}")

def process_repo(repo_num, repo, table_directory, df_watchers, df_issues, df_followers, df_commits):
    repo_name = repo["name"]
    contributor_file = f"../FilteredContributors/contributors_{repo_name}.json"
    csv_path = f"{table_directory}/dp_{repo_name}.csv"

    try:
        with open(contributor_file, 'r', encoding='utf-8') as f:
            contributor_list = json.load(f)

        create_developer_profile(repo_num, repo, contributor_list, csv_path,
                                 df_watchers, df_issues, df_followers, df_commits)
    except Exception as e:
        print(f"[Error] Repo {repo_name}: {e}")

def main():
    table_directory = "../Tables/DeveloperProfiles"
    os.makedirs(table_directory, exist_ok=True)

    # Read all needed CSVs and parse datetimes
    df_watchers = pl.read_csv("../GHTorrent Data/watchers_filtered.csv").with_columns(
        pl.col("created_at").str.strptime(pl.Datetime, strict=False).dt.replace_time_zone("UTC")
    )
    df_issues = pl.read_csv("../GHTorrent Data/issues_filtered.csv").with_columns(
        pl.col("created_at").str.strptime(pl.Datetime, strict=False).dt.replace_time_zone("UTC")
    )
    df_followers = pl.read_csv("../GHTorrent Data/follower_filtered.csv").with_columns(
        pl.col("created_at").str.strptime(pl.Datetime, strict=False).dt.replace_time_zone("UTC")
    )
    
    df_followers = pl.read_csv("../GHTorrent Data/follower_filtered.csv").with_columns(
        pl.col("created_at").str.strptime(pl.Datetime, strict=False).dt.replace_time_zone("UTC")
    )
    df_commits = pl.read_csv("../GHTorrent Data/commits_filtered.csv").with_columns(
        pl.col("created_at").str.strptime(pl.Datetime, strict=False).dt.replace_time_zone("UTC")
    )
    # print(df_commits.head())

    # Load repo list
    with open('../filteredRepos.json', 'r', encoding='utf-8') as f:
        repo_list = json.load(f)

    # Run processing in parallel using threads
    with ThreadPoolExecutor(max_workers=3) as executor:  # You can increase this number
        futures = [
            executor.submit(process_repo, i, repo, table_directory,
                            df_watchers, df_issues, df_followers, df_commits)
            for i, repo in enumerate(repo_list[:121])
            # 121
        ]
        for future in futures:
            future.result()  # Raise errors if any

if __name__ == "__main__":
    # remaining, reset_time = check_rate_limit()
    main()      
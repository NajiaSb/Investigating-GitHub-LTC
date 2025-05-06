""" This code collects data for the Developer Monthly Activity feature table.
"""
import os
import json
import time
import requests
import pandas as pd
import polars as pl
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor

load_dotenv()
MAX_GHTORRENT_DATE = pd.to_datetime("2021-03-06 23:57:37+00:00")  # Max date from GHTorrent data
GITHUB_TOKEN = os.getenv("GIT_TOKEN1")
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
    collected_data = []
    while url:
        remaining, reset_time = check_rate_limit()
        if remaining == 0:
            wait_time = max(1, reset_time - time.time())
            print(f"\t\tRate limit exceeded! Waiting {wait_time:.2f} seconds before retrying...")
            time.sleep(wait_time + 5)
            continue
        
        response = requests.get(url, headers=HEADERS)    
        if response.status_code != 200:
            # print(f"\tStatus Code: {response.status_code}, {url}...")
            break
        
        data = response.json()
        collected_data.extend(data)
        url = response.links.get('next', {}).get('url')
    return collected_data

def create_developer_monthly_activity(repo_num, repo_id, repo_name, repo_language, contributor_list, commits, 
                                      commit_comments, issues, issue_comments, issue_events, pull_requests, 
                                      pull_request_comments, pulls_events):
    # Define CSV columns
    columns = [
        "repo_name", "repo_id", "user_id", "registration_date", "language", 
        "month_user_commits", "month_user_commit_comments",
        "month_user_issues", "month_user_issue_comments",
        "month_user_issue_events", "month_user_issue_events_closed", "month_user_issue_events_assigned",
        "month_user_pull_requests", "month_user_pull_request_comments",
        "month_user_pull_request_history", 
        "month_user_pull_request_history_merged", "month_user_pull_request_history_closed", 
        "ltc_1", "ltc_2", "ltc_3"
    ]  
    monthly_activity_directory = f"../Tables/DeveloperMonthlyActivity/dma_{repo_name}.csv"
    file_exists = os.path.isfile(monthly_activity_directory)
    # Load existing data if the file exists
    if file_exists:
        existing_df = pd.read_csv(monthly_activity_directory)
    else:
        existing_df = pd.DataFrame(columns=columns)
    monthly_activity_data = []

    for i, developer in enumerate(contributor_list):
        developer_id = developer["id"]
        username = developer["login"]
        developer_date = pd.to_datetime(developer["registration_date"])
        one_month_later = developer_date + pd.DateOffset(months=1)
        ltc_1 = 1 if developer["one_year"] == "yes" else 0
        ltc_2 = 1 if developer["one_year"] == "yes" and developer["two_years"] == "yes" else 0
        ltc_3 = 1 if developer["LTC"] == "yes" else 0
        
        # Check if user already exists in the CSV
        if file_exists and developer_id in existing_df["user_id"].values:
            print(f"{repo_num}: {repo_name} - User {username} already exists in dataset, skipping...")
            continue
        elif developer_date >= MAX_GHTORRENT_DATE:
            print(f"{repo_num}: {repo_name} - User {username} joined in {developer_date}, skipping...")
            continue
        else:
            print(f"{repo_num}: {repo_name} - Processing {i}: {developer["login"]} out of {len(contributor_list)}")

        # Initialize activity metrics
        month_user_commits = 0
        month_user_commit_comments = 0
        month_user_issues = 0
        month_user_issue_comments = 0
        month_user_issue_events = 0
        month_user_issue_events_closed = 0
        month_user_issue_events_assigned = 0
        month_user_pull_requests = 0
        month_user_pull_request_comments = 0
        month_user_pull_request_history = 0
        month_user_pull_request_history_merged = 0
        month_user_pull_request_history_closed = 0

        # Count commits made by the developer in the first month
        commits_sha = set()
        for commit in commits:
            committer = commit.get("author")
            commit_date = commit.get("commit", {})
            commit_sha = commit.get("sha")
            if not committer or not commit_date:
                continue
            committer_id = committer.get("id")
            commit_date = pd.to_datetime(commit_date.get("author", {}).get("date"))
            if developer_date <= commit_date < one_month_later and committer_id == developer_id:
                if commit_sha not in commits_sha:
                    commits_sha.add(commit_sha)
                month_user_commits += 1 # FEATURE 1

        # Count comments received on developer's commits in the first month
        for commit_cmt in commit_comments:
            commit_cmt_date = pd.to_datetime(commit_cmt.get("created_at"))
            commit_id = commit_cmt.get("commit_id")
            if not commit_cmt_date:
                continue
            if developer_date <= commit_cmt_date < one_month_later and commit_id in commits_sha:
                month_user_commit_comments += 1 # FEATURE 2
     
        # Count issues submitted by the developer in the first month 
        issue_ids = set()
        issue_urls = set()
        for issue in issues:
            issue_creator_id = issue.get("user", {})
            issue_date = pd.to_datetime(issue.get("created_at"))
            issue_url = issue.get("events_url")
            issue_id = issue.get("id")
            if not issue_creator_id or not issue_date:
                continue
            issue_creator_id = issue_creator_id.get("id")
            if developer_date <= issue_date < one_month_later and issue_creator_id == developer_id: # Get issues of developer in first month
                month_user_issues += 1 # FEATURE 3
                if issue_id not in issue_ids and issue_date <= MAX_GHTORRENT_DATE: 
                    issue_ids.add(issue_id)
                elif issue_url not in issue_urls and issue_date > MAX_GHTORRENT_DATE:
                    issue_urls.add(issue_url)
                
        # Count issue events received in developer's issues in the first month
        if issue_ids: # get events from GHTorrent
            issue_filtered = issue_events.filter(
                (pl.col("issue_id").is_in(issue_ids)) 
                & (developer_date <= issue_events["created_at"]) 
                & (issue_events["created_at"] < one_month_later)
            )     
            month_user_issue_events+=issue_filtered.height
            # get assigned events
            assigned_events = issue_filtered.filter(issue_filtered["action"] == "assigned")
            month_user_issue_events_assigned+=assigned_events.height
            # get closed events
            closed_events = issue_filtered.filter(issue_filtered["action"] == "closed")
            month_user_issue_events_closed+=closed_events.height
        if issue_urls:  # get events from GitHub API
            print("\tGetting from API")
            for url in issue_urls:
                issue_events_fetched = get_github_data(url) # get specific event data
                for issue_event in issue_events_fetched:
                    event_type = issue_event.get("event")
                    event_date = pd.to_datetime(issue_event.get("created_at"))
                    if developer_date <= event_date < one_month_later:
                        month_user_issue_events += 1
                        if event_type == "assigned":
                            month_user_issue_events_assigned += 1
                        if event_type == "closed":
                            month_user_issue_events_closed += 1
        
        # Count comments received on developer's issues in the first month
        for issue_cmt in issue_comments:
            issue_cmt_date = pd.to_datetime(issue_cmt.get("created_at"))
            issue_author_id = issue_cmt.get("issue", {})
            issue_url = issue_cmt.get("issue_url")
            if not issue_cmt_date or not issue_author_id:
                continue
            issue_author_id = issue_author_id.get("user", {}).get("id")
            if developer_date <= issue_cmt_date < one_month_later and issue_url in issue_urls:
                month_user_issue_comments += 1 # FEATURE 4
        
        # Count pull requests submitted by the developer in the first month
        pulls_urls = set()
        pulls_ids = set()
        for pull_request in pull_requests:
            pull_request_user_id = pull_request.get("user", {}) # Id of pull user
            pull_request_date = pd.to_datetime(pull_request.get("created_at")) # Pull date
            pull_url = pull_request.get("url")
            pull_id = pull_request.get("id")
            if not pull_request_user_id or not pull_request_date:
                continue
            pull_request_user_id = pull_request_user_id.get("id")
            if developer_date <= pull_request_date < one_month_later and pull_request_user_id != developer_id:
                month_user_pull_requests += 1 # FEATURE 9
                if pull_id not in pulls_ids and pull_request_date <= MAX_GHTORRENT_DATE: 
                    pulls_ids.add(pull_id)
                elif pull_url not in pulls_urls and pull_request_date > MAX_GHTORRENT_DATE:
                    pulls_urls.add(pull_url)
        
        if pulls_ids:
            # filter for events within first month
            issue_filtered = pulls_events.filter((pl.col("pull_request_id").is_in(pulls_ids)) & 
                                        (developer_date <= pulls_events["created_at"]) & 
                                        (pulls_events["created_at"] < one_month_later))  
            month_user_pull_request_history+=issue_filtered.height
            # get assigned events
            merged_events = issue_filtered.filter(issue_filtered["action"] == "merged")
            month_user_pull_request_history_merged+=merged_events.height
            # get closed events
            closed_events = issue_filtered.filter(issue_filtered["action"] == "closed")
            month_user_pull_request_history_closed+=closed_events.height
        if pulls_urls:
            print("\tGetting from API")
            for url in pulls_urls: 
                pull_events = get_github_data(url + "/events") # get specific event data
                for pull_event in pull_events:
                    event_type = pull_event.get("event")
                    event_date = pd.to_datetime(pull_event.get("created_at"))
                    if developer_date <= event_date < one_month_later:
                        month_user_pull_request_history += 1
                        if event_type == "assigned":
                            month_user_pull_request_history_merged += 1
                        if event_type == "closed":
                            month_user_pull_request_history_closed += 1   

        # Count comments received on developer's pull requests in the first month
        for pull_request_cmt in pull_request_comments:
            pull_cmt_date = pd.to_datetime(pull_request_cmt.get("created_at"))
            pull_url = pull_request_cmt.get("pull_request_url") # parent url
            if developer_date <= pull_cmt_date < one_month_later and pull_url in pulls_urls:
                month_user_pull_request_comments += 1 # FEATURE 10

        # Store developer's monthly activity data
        monthly_activity_data.append([
            repo_name, repo_id, developer_id, developer_date.date(), repo_language, 
            month_user_commits, month_user_commit_comments,
            month_user_issues, month_user_issue_comments,
            month_user_issue_events, month_user_issue_events_closed, month_user_issue_events_assigned,
            month_user_pull_requests, month_user_pull_request_comments,
            month_user_pull_request_history, month_user_pull_request_history_merged, 
            month_user_pull_request_history_closed, ltc_1, ltc_2, ltc_3
        ])
        # Save to csv
        temp_df = pd.DataFrame(monthly_activity_data, columns=columns)
        temp_df.to_csv(monthly_activity_directory, mode="a", header=not file_exists, index=False)
        file_exists = True  # Ensure header isn't written again after first write
        monthly_activity_data = []  # Clear new data list

def process_repo(repo_num, repo, df_issue_events, df_pulls_events):
    repo_id = repo["id"]
    repo_name = repo["name"]
    repo_language = repo["language"]

    with open(f"../FilteredContributors/contributors_{repo_name}.json", "r", encoding="utf-8") as f:
        contributor_list = json.load(f)
    with open(f"../Datasets/{repo_name}/commits_{repo_name}.json", "r", encoding="utf-8") as f:
        commits = json.load(f)
    with open(f"../Datasets/{repo_name}/commit_comments_{repo_name}.json", "r", encoding="utf-8") as f:
        commit_comments = json.load(f)
    with open(f"../Datasets/{repo_name}/issues_{repo_name}.json", "r", encoding="utf-8") as f:
        issues = json.load(f)
    with open(f"../Datasets/{repo_name}/issue_comments_{repo_name}.json", "r", encoding="utf-8") as f:
        issue_comments = json.load(f)
    with open(f"../Datasets/{repo_name}/pull_requests_{repo_name}.json", "r", encoding="utf-8") as f:
        pull_requests = json.load(f)
    with open(f"../Datasets/{repo_name}/pull_request_comments_{repo_name}.json", "r", encoding="utf-8") as f:
        pull_request_comments = json.load(f)

    create_developer_monthly_activity(repo_num, repo_id, repo_name, repo_language, contributor_list, commits,
                                      commit_comments, issues, issue_comments, df_issue_events,
                                      pull_requests, pull_request_comments, df_pulls_events)

def main():
    monthly_activity_directory = "../Tables/DeveloperMonthlyActivity"
    os.makedirs(monthly_activity_directory, exist_ok=True)

    with open("../filteredRepos.json", "r", encoding="utf-8") as f:
        repo_list = json.load(f)

    df_issue_events = pl.read_csv("../GHTorrent Data/issue_events_filtered.csv").with_columns(
        pl.col("created_at").str.strptime(pl.Datetime, strict=False).dt.replace_time_zone("UTC")
    )

    df_pulls_events = pl.read_csv("../GHTorrent Data/pull_events_filtered.csv").with_columns(
        pl.col("created_at").str.strptime(pl.Datetime, strict=False).dt.replace_time_zone("UTC")
    )

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [
            executor.submit(process_repo, i, repo, df_issue_events, df_pulls_events)
            for i, repo in enumerate(repo_list[49:102])
        ]
        for future in futures:
            future.result()

if __name__ == "__main__":
    main()
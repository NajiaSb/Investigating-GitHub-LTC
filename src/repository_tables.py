""" This code collects data for both Repository feature table.
"""
import os
import json
import time
import requests
import pandas as pd
import polars as pl
import statistics
from collections import OrderedDict
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor

load_dotenv()
MAX_GHTORRENT_DATE = pd.to_datetime("2021-03-06 23:57:37+00:00")  # Max date from GHTorrent data
GITHUB_TOKEN = os.getenv("GIT_TOKEN2")
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
            break
        
        data = response.json()
        collected_data.extend(data)
        url = response.links.get('next', {}).get('url')
    return collected_data

def create_repository_profile(repo_num, repo_id, repo_name, repo_language, contributor_list, commits, 
                              commit_comments, issues, issue_comments, issue_events, pull_requests, 
                              pull_request_comments, pulls_events, watchers):
    # Define CSV columns
    columns1 = [
        "repo_name", "repo_id", "user_id", "registration_date", "language", "before_repo_commits", "before_repo_commit_comments", "before_repo_contributors", 
        "before_repo_contributor_max", "before_repo_contributor_min", "before_repo_contributor_mean", "before_repo_contributor_std",
        "before_repo_contributor_median", "before_repo_issues", "before_repo_issue_comments", "before_repo_issue_events", 
        "before_repo_issue_events_closed", "before_repo_issue_events_assigned",
        "before_repo_pull_requests", "before_repo_pull_request_comments", "before_repo_pull_request_history",
        "before_repo_pull_request_history_merged", "before_repo_pull_request_history_closed", "before_repo_watchers", "ltc_1", "ltc_2", "ltc_3"
    ]
    columns2 = [
        "repo_name","repo_id", "user_id", "registration_date", "language", 
        "month_repo_commits", "month_repo_commit_comments", "month_repo_contributors", 
        "month_repo_contributor_max", "month_repo_contributor_min", "month_repo_contributor_mean", 
        "month_repo_contributor_std", "month_repo_contributor_std", "month_repo_contributor_median", "month_repo_issues", 
        "month_repo_issue_comments", "month_repo_issue_events", "month_repo_issue_events_closed", "month_repo_issue_events_assigned",
        "month_repo_pull_requests", "month_repo_pull_request_comments", "month_repo_pull_request_history", 
        "month_repo_pull_request_history_merged", "month_repo_pull_request_history_closed", "ltc_1", "ltc_2", "ltc_3"
    ]
    
    repository_profile_directory = f"../Tables/RepositoryProfiles/rp_{repo_name}.csv"
    file_exists1 = os.path.isfile(repository_profile_directory)
     # Load existing data if the file exists
    if file_exists1:
        existing_df1 = pd.read_csv(repository_profile_directory)
    else:
        existing_df1 = pd.DataFrame(columns=columns1)
        
    repo_activity_directory = f"../Tables/RepositoryMonthlyActivity/rma_{repo_name}.csv"
    file_exists2 = os.path.isfile(repo_activity_directory)

    repository_profile = []
    repo_activity_data = []
    
    for i, developer in enumerate(contributor_list):
        developer_id = developer["id"]
        username = developer["login"]
        developer_date = pd.to_datetime(developer["registration_date"])
        ltc_1 = 1 if developer["one_year"] == "yes" else 0
        ltc_2 = 1 if developer["one_year"] == "yes" and developer["two_years"] == "yes" else 0
        ltc_3 = 1 if developer["LTC"] == "yes" else 0
        
        # Check if user already exists in the CSV
        if file_exists1 and developer_id in existing_df1["user_id"].values:
            print(f"{repo_num}: {repo_name} - User {username} already exists in dataset, skipping...")
            continue
        elif developer_date >= MAX_GHTORRENT_DATE:
            print(f"{repo_num}: {repo_name} - User {username} joined in {developer_date}, skipping...")
            continue
        else:
            print(f"{repo_num}: {repo_name} - Processing {i}: {username} out of {len(contributor_list)}")
        
        # Initialize for table 1
        before_repo_commits = 0
        before_repo_commit_comments = 0
        before_repo_contributors = 0
        before_repo_issues = 0
        before_repo_issue_comments = 0
        before_repo_issue_events = 0
        before_repo_issue_events_closed = 0
        before_repo_issue_events_assigned = 0
        before_repo_pull_requests = 0
        before_repo_pull_request_comments = 0
        before_repo_pull_request_history = 0
        before_repo_pull_request_history_merged = 0
        before_repo_pull_request_history_closed = 0
        before_repo_watchers = 0
        
        # Initialize for table 2
        one_month_later = developer_date + pd.DateOffset(months=1)
        month_repo_commits = 0
        month_repo_commit_comments = 0
        month_repo_contributors = 0
        month_repo_issues = 0
        month_repo_issue_comments = 0
        month_repo_issue_events = 0
        month_repo_issue_events_closed = 0
        month_repo_issue_events_assigned = 0
        month_repo_pull_requests = 0
        month_repo_pull_request_history = 0
        month_repo_pull_request_comments = 0
        month_repo_pull_request_history_merged = 0
        month_repo_pull_request_history_closed = 0

        # Count commits before developer joins
        repo_profile_commits = []
        repo_month_commits = []
        
        for commit in commits:
            committer = commit.get("author", {})
            commit_date = commit.get("commit", {})
            if not committer or not commit_date:
                continue
            committer_id = committer.get("id") # commit id
            commit_date = pd.to_datetime(commit_date.get("author", {}).get("date")) # commit date
            # all commits made before author joins
            if commit_date <= developer_date and committer_id != developer_id:
                repo_profile_commits.append(commit)
                before_repo_commits += 1
            if developer_date <= commit_date < one_month_later and committer_id != developer_id:
                repo_month_commits.append(commit)
                month_repo_commits += 1

        # Count commit comments before developer joins
        for commit_cmt in commit_comments:
            commit_cmt_id = commit_cmt.get("user", {})
            commit_cmt_date = pd.to_datetime(commit_cmt.get("created_at"))
            if not commit_cmt_id or not commit_cmt_date:
                continue
            commit_cmt_id = commit_cmt_id.get("id")
            if commit_cmt_date <= developer_date and commit_cmt_id != developer_id:
                before_repo_commit_comments += 1
            if developer_date <= commit_cmt_date < one_month_later and commit_cmt_id != developer_id:
                month_repo_commit_comments += 1

        # Count contributors before developer joins
        for contributor in contributor_list:
            contributor_id = contributor["id"]
            contributor_date = pd.to_datetime(contributor["registration_date"])
            if not contributor_id or not contributor_date:
                continue
            if contributor_date <= developer_date and contributor_id != developer_id:
                before_repo_contributors += 1
            if developer_date <= contributor_date < one_month_later and contributor_id != developer_id:
                month_repo_contributors += 1
        
        # Get commit statistics
        repo_profile_stats = OrderedDict()
        for commit in repo_profile_commits:
            committer = commit.get("author", {}).get("id")
            if committer in repo_profile_stats:
                repo_profile_stats[committer] += 1  # Increment count if already present
            else:
                repo_profile_stats[committer] = 1  # Initialize count
        occurrences = list(repo_profile_stats.values()) # Extract values (occurrences)
        if occurrences:
            before_repo_contributor_max = round(max(occurrences))
            before_repo_contributor_min = round(min(occurrences))
            before_repo_contributor_mean = round(statistics.mean(occurrences))
            before_repo_contributor_std = round(statistics.stdev(occurrences)) if len(occurrences) > 1 else 0
            before_repo_contributor_median = round(statistics.median(occurrences))
        else:
            # Provide default values if occurrences is empty
            before_repo_contributor_max = 0
            before_repo_contributor_min = 0
            before_repo_contributor_mean = 0
            before_repo_contributor_std = 0
            before_repo_contributor_median = 0
        # Get month commit statistics
        repo_month_stats = OrderedDict()
        for commit in repo_month_commits:
            committer = commit.get("author", {}).get("id")
            if committer in repo_month_stats:
                repo_month_stats[committer] += 1  # Increment count if already present
            else:
                repo_month_stats[committer] = 1  # Initialize count
        occurrences = list(repo_month_stats.values()) # Extract values (occurrences)
        if occurrences:
            month_repo_contributor_max = round(max(occurrences))
            month_repo_contributor_min = round(min(occurrences))
            month_repo_contributor_mean = round(statistics.mean(occurrences))
            month_repo_contributor_std = round(statistics.stdev(occurrences)) if len(occurrences) > 1 else 0
            month_repo_contributor_median = round(statistics.median(occurrences))
        else:
            # Provide default values if occurrences is empty
            month_repo_contributor_max = 0
            month_repo_contributor_min = 0
            month_repo_contributor_mean = 0
            month_repo_contributor_std = 0
            month_repo_contributor_median = 0

        # Initialize sets to track issues
        issue_urls = set()
        month_urls = set()
        issue_ids = set()
        month_issue_ids = set()

        for issue in issues:
            issue_url = issue.get("events_url")
            issue_user = issue.get("user", {})
            issue_date = pd.to_datetime(issue.get("created_at"))
            issue_id = issue.get("id")

            if not issue_user or not issue_date:
                continue

            issue_creator_id = issue_user.get("id")  # Get issue creator ID

            # Count issues before developer joins
            if issue_date <= developer_date and issue_creator_id != developer_id:
                before_repo_issues += 1
                if issue_id not in issue_ids and issue_date <= MAX_GHTORRENT_DATE:
                    issue_ids.add(issue_id)
                elif (issue_id, issue_url, issue_date) not in issue_urls and issue_date > MAX_GHTORRENT_DATE:
                    issue_urls.add((issue_id, issue_url, issue_date))

            # Count issues within the first month after developer joins
            if developer_date <= issue_date < one_month_later and issue_creator_id != developer_id:
                month_repo_issues += 1
                if issue_id not in month_issue_ids and issue_date <= MAX_GHTORRENT_DATE:
                    month_issue_ids.add(issue_id)
                elif (issue_id, issue_url, issue_date) not in month_urls and issue_date > MAX_GHTORRENT_DATE:
                    month_urls.add((issue_id, issue_url, issue_date))

        """
        Process issue events for issues before developer joins
        """
        if issue_ids:  # Get events from GHTorrent
            issue_filtered = issue_events.filter(
                (pl.col("issue_id").is_in(issue_ids)) &
                (issue_events["created_at"] <= developer_date)
            )
            before_repo_issue_events += issue_filtered.height
            assigned_events = issue_filtered.filter(issue_filtered["action"] == "assigned")
            before_repo_issue_events_assigned += assigned_events.height
            closed_events = issue_filtered.filter(issue_filtered["action"] == "closed")
            before_repo_issue_events_closed += closed_events.height

        if issue_urls:  # Fetch events from GitHub API
            print("\tGetting from API")
            for issue_id, url, date in issue_urls:
                issue_events_fetched = get_github_data(url)
                for issue_event in issue_events_fetched:
                    event_type = issue_event.get("event")
                    event_date = pd.to_datetime(issue_event.get("created_at"))
                    if event_date <= developer_date:
                        before_repo_issue_events += 1
                        if event_type == "assigned":
                            before_repo_issue_events_assigned += 1
                        if event_type == "closed":
                            before_repo_issue_events_closed += 1

        """
        Process issue events for issues in the first month after developer joins
        """
        if month_issue_ids:  # Get events from GHTorrent
            month_issue_filtered = issue_events.filter(
                (pl.col("issue_id").is_in(month_issue_ids)) &
                (developer_date <= issue_events["created_at"]) &
                (issue_events["created_at"] < one_month_later)
            )
            month_repo_issue_events += month_issue_filtered.height
            assigned_events = month_issue_filtered.filter(month_issue_filtered["action"] == "assigned")
            month_repo_issue_events_assigned += assigned_events.height
            closed_events = month_issue_filtered.filter(month_issue_filtered["action"] == "closed")
            month_repo_issue_events_closed += closed_events.height

        if month_urls:  # Fetch events from GitHub API
            print("\tGetting from API")
            for issue_id, url, date in month_urls:
                issue_events_fetched = get_github_data(url)
                for issue_event in issue_events_fetched:
                    event_type = issue_event.get("event")
                    event_date = pd.to_datetime(issue_event.get("created_at"))
                    if developer_date <= event_date < one_month_later:
                        month_repo_issue_events += 1
                        if event_type == "assigned":
                            month_repo_issue_events_assigned += 1
                        if event_type == "closed":
                            month_repo_issue_events_closed += 1


        # Count issue comments before developer joins
        for issue_cmt in issue_comments:
            issue_cmt_id = issue_cmt.get("user", {})
            issue_cmt_date = pd.to_datetime(issue_cmt.get("created_at"))
            if not issue_cmt_id or not issue_cmt_date:
                continue
            issue_cmt_id = issue_cmt_id.get("id")
            if issue_cmt_date <= developer_date and issue_cmt_id != developer_id:
                before_repo_issue_comments += 1
            if developer_date <= issue_cmt_date < one_month_later and issue_cmt_id != developer_id:
                month_repo_issue_comments += 1 

        # Initialize sets to track pull requests
        pulls_url_before = set()
        pulls_url_month = set()
        pull_ids_before = set()
        pull_ids_month = set()

        # Process pull requests for "before" and "month" ranges
        for pull_request in pull_requests:
            pull_request_user = pull_request.get("user", {})
            pull_request_date = pd.to_datetime(pull_request.get("created_at"))
            pull_url = pull_request.get("url")
            pull_id = pull_request.get("id")

            if not pull_request_user or not pull_request_date:
                continue 

            pull_request_user_id = pull_request_user.get("id")  # Get user ID

            # Process pull requests before the developer joins
            if pull_request_date <= developer_date and pull_request_user_id != developer_id:
                before_repo_pull_requests += 1
                if pull_id not in pull_ids_before and pull_request_date <= MAX_GHTORRENT_DATE:
                    pull_ids_before.add(pull_id)
                elif (pull_id, pull_url, pull_request_date) not in pulls_url_before and pull_request_date > MAX_GHTORRENT_DATE:
                    pulls_url_before.add(pull_url)

            # Process pull requests within the month after the developer joins
            if developer_date <= pull_request_date < one_month_later and pull_request_user_id != developer_id:
                month_repo_pull_requests += 1
                if pull_id not in pull_ids_month and pull_request_date <= MAX_GHTORRENT_DATE:
                    pull_ids_month.add(pull_id)
                elif (pull_id, pull_url, pull_request_date) not in pulls_url_month and pull_request_date > MAX_GHTORRENT_DATE:
                    pulls_url_month.add(pull_url)

        """
        Process pull request events for "before" range
        """
        if pull_ids_before:  # Get events from GHTorrent
            pulls_filtered = pulls_events.filter(
                (pl.col("pull_request_id").is_in(pull_ids_before)) &
                (pulls_events["created_at"] <= developer_date)
            )
            before_repo_pull_request_history += pulls_filtered.height
            merged_events = pulls_filtered.filter(pulls_filtered["action"] == "merged")
            before_repo_pull_request_history_merged += merged_events.height
            closed_events = pulls_filtered.filter(pulls_filtered["action"] == "closed")
            before_repo_pull_request_history_closed += closed_events.height

        if pulls_url_before:  # Fetch events from GitHub API
            print("\tGetting from API")
            for url in pulls_url_before:
                pull_events_fetched = get_github_data(url + "/events")
                for pull_event in pull_events_fetched:
                    event_type = pull_event.get("event")
                    event_date = pd.to_datetime(pull_event.get("created_at"))
                    if event_date <= developer_date:
                        before_repo_pull_request_history += 1
                        if event_type == "merged":
                            before_repo_pull_request_history_merged += 1
                        if event_type == "closed":
                            before_repo_pull_request_history_closed += 1

        """
        Process pull request events for "month" range
        """
        if pull_ids_month:  # Get events from GHTorrent
            pull_filtered = pulls_events.filter(
                (pl.col("pull_request_id").is_in(pull_ids_month)) & 
                (developer_date <= pulls_events["created_at"]) & 
                (pulls_events["created_at"] < one_month_later)
            )
            month_repo_pull_request_history += pull_filtered.height
            merged_events = pull_filtered.filter(pull_filtered["action"] == "merged")
            month_repo_pull_request_history_merged += merged_events.height
            closed_events = pull_filtered.filter(pull_filtered["action"] == "closed")
            month_repo_pull_request_history_closed += closed_events.height

        if pulls_url_month:  # Fetch events from GitHub API
            print("\tGetting from API")
            for url in pulls_url_month:
                pull_events_fetched = get_github_data(url + "/events")
                for pull_event in pull_events_fetched:
                    event_type = pull_event.get("event")
                    event_date = pd.to_datetime(pull_event.get("created_at"))
                    if developer_date <= event_date < one_month_later:
                        month_repo_pull_request_history += 1
                        if event_type == "merged":
                            month_repo_pull_request_history_merged += 1
                        if event_type == "closed":
                            month_repo_pull_request_history_closed += 1

        # Process pull request comments for both "before" and "month" ranges
        for pull_request_cmt in pull_request_comments:
            pull_cmt_id = pull_request_cmt.get("user", {})  # Comment user id
            pull_cmt_date = pd.to_datetime(pull_request_cmt.get("created_at"))  # Comment date
            if not pull_cmt_id or not pull_cmt_date:
                continue
            pull_cmt_id = pull_cmt_id.get("id")  # Get the comment user id
            # Check for comments within the month before developer joins
            if developer_date <= pull_cmt_date < one_month_later and pull_cmt_id != developer_id:
                month_repo_pull_request_comments += 1
            # Check for comments before the developer joins
            if pull_cmt_date <= developer_date and pull_cmt_id != developer_id:
                before_repo_pull_request_comments += 1
                
        """Process Watchers Data
        """
        watches_filtered = watchers.filter(
            (pl.col("repo_id") == repo_id) & 
            (pl.col("created_at") <= developer_date)        
        )
        before_repo_watchers += watches_filtered.height

        # Store data for this contributor
        repository_profile.append([
            repo_name, repo_id, developer_id, developer_date.date(), repo_language, before_repo_commits, before_repo_commit_comments, before_repo_contributors,
            before_repo_contributor_max, before_repo_contributor_min, before_repo_contributor_mean, before_repo_contributor_std,
            before_repo_contributor_median, before_repo_issues, before_repo_issue_comments, before_repo_issue_events, 
            before_repo_issue_events_closed, before_repo_issue_events_assigned, before_repo_pull_requests,
            before_repo_pull_request_comments, before_repo_pull_request_history,
            before_repo_pull_request_history_merged, before_repo_pull_request_history_closed, before_repo_watchers, ltc_1, ltc_2, ltc_3
        ])
        # Save to csv
        temp_df = pd.DataFrame(repository_profile, columns=columns1)
        temp_df.to_csv(repository_profile_directory, mode="a", header=not file_exists1, index=False)
        file_exists1 = True  # Ensure header isn't written again after first write
        repository_profile = []  # Clear new data list
        
        # Store repository's monthly activity data
        repo_activity_data.append([
            repo_name, repo_id, developer_id, developer_date.date(), repo_language, month_repo_commits, month_repo_commit_comments, month_repo_contributors,
            month_repo_contributor_max, month_repo_contributor_min, month_repo_contributor_mean, month_repo_contributor_std, month_repo_contributor_std, month_repo_contributor_median,
            month_repo_issues, month_repo_issue_comments,
            month_repo_issue_events, month_repo_issue_events_closed, month_repo_issue_events_assigned,
            month_repo_pull_requests, month_repo_pull_request_comments,
            month_repo_pull_request_history, month_repo_pull_request_history_merged, month_repo_pull_request_history_closed, ltc_1, ltc_2, ltc_3
        ])
        # Save to csv
        temp_df = pd.DataFrame(repo_activity_data, columns=columns2)
        temp_df.to_csv(repo_activity_directory, mode="a", header=not file_exists2, index=False)
        file_exists2 = True  # Ensure header isn't written again after first write
        repo_activity_data = []  # Clear new data list

def process_repo(repo_num, repo, df_issue_events, df_pulls_events, df_watchers):
    repo_id = repo["id"]
    repo_name = repo["name"]
    repo_language = repo["language"]

    try:
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

        create_repository_profile(
            repo_num, repo_id, repo_name, repo_language,
            contributor_list, commits, commit_comments, issues, issue_comments,
            df_issue_events, pull_requests, pull_request_comments,
            df_pulls_events, df_watchers
        )
    except Exception as e:
        print(f"[Error] Failed processing repo {repo_name}: {e}")

def main():
    # Define and create directories
    directories = [
        "../Tables/RepositoryProfiles",
        "../Tables/RepositoryMonthlyActivity"
    ]
    for directory in directories:
        os.makedirs(directory, exist_ok=True)

    # Load CSVs and parse datetime
    df_issue_events = pl.read_csv("../GHTorrent Data/issue_events_filtered.csv").with_columns(
        pl.col("created_at").str.strptime(pl.Datetime, strict=False).dt.replace_time_zone("UTC")
    )
    df_pulls_events = pl.read_csv("../GHTorrent Data/pull_events_filtered.csv").with_columns(
        pl.col("created_at").str.strptime(pl.Datetime, strict=False).dt.replace_time_zone("UTC")
    )
    df_watchers = pl.read_csv("../GHTorrent Data/watchers_filtered.csv").with_columns(
        pl.col("created_at").str.strptime(pl.Datetime, strict=False).dt.replace_time_zone("UTC")
    )

    # Load repository list
    with open("../filteredRepos.json", "r", encoding="utf-8") as f:
        repo_list = json.load(f)

    # Run concurrently using threads
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(process_repo, i, repo, df_issue_events, df_pulls_events, df_watchers)
            for i, repo in enumerate(repo_list[90:102])
        ]
        for future in futures:
            future.result()

if __name__ == "__main__":
    main()

# def main():
#     # Define directories
#     directories = [
#         "../Tables/RepositoryProfiles",
#         "../Tables/RepositoryMonthlyActivity"
#     ]
#     # Create directories if they don't exist
#     for directory in directories:
#         os.makedirs(directory, exist_ok=True)
        
#     # Open GHTorrent issue_events
#     df_issue_events = pl.read_csv("../GHTorrent Data/issue_events_filtered.csv")
#     df_issue_events = df_issue_events.with_columns(
#         pl.col("created_at")
#         .str.strptime(pl.Datetime, strict=False)  # Convert from string to datetime
#         .dt.replace_time_zone("UTC")  # Make it timezone-aware
#     )

#     # Open GHTorrent pull_events
#     df_pulls_events = pl.read_csv("../GHTorrent Data/pull_events_filtered.csv")
#     df_pulls_events = df_pulls_events.with_columns(
#         pl.col("created_at")
#         .str.strptime(pl.Datetime, strict=False)  # Convert from string to datetime
#         .dt.replace_time_zone("UTC")  # Make it timezone-aware
#     )

#     # Open GHTorrent watchers
#     df_watchers = pl.read_csv("../GHTorrent Data/watchers_filtered.csv")
#     df_watchers = df_watchers.with_columns(
#         pl.col("created_at")
#         .str.strptime(pl.Datetime, strict=False)  # Convert from string to datetime
#         .dt.replace_time_zone("UTC")  # Make it timezone-aware
#     )

#     # Open list of all repos
#     with open("../filteredRepos.json", "r", encoding="utf-8") as f:
#         repo_list = json.load(f)
#     # Iterate through repo list
#     for repo_num, repo in enumerate(repo_list):
#         repo_id = repo["id"]
#         repo_name = repo["name"]
#         repo_language = repo["language"]
#         # Get contributor list
#         with open(f"../FilteredContributors/contributors_{repo_name}.json", "r", encoding="utf-8") as f:
#             contributor_list = json.load(f)
#         # Get commit list   
#         with open(f"../Datasets/{repo_name}/commits_{repo_name}.json", "r", encoding="utf-8") as f:
#             commits = json.load(f)
#         # Get commit comment list    
#         with open(f"../Datasets/{repo_name}/commit_comments_{repo_name}.json", "r", encoding="utf-8") as f:
#             commit_comments = json.load(f)
#         # Get issues list   
#         with open(f"../Datasets/{repo_name}/issues_{repo_name}.json", "r", encoding="utf-8") as f:
#             issues = json.load(f)
#         # Get issue comment list   
#         with open(f"../Datasets/{repo_name}/issue_comments_{repo_name}.json", "r", encoding="utf-8") as f:
#             issue_comments = json.load(f)
#         # Get pull list
#         with open(f"../Datasets/{repo_name}/pull_requests_{repo_name}.json", "r", encoding="utf-8") as f:
#             pull_requests = json.load(f)
#         # Get pull list comment
#         with open(f"../Datasets/{repo_name}/pull_request_comments_{repo_name}.json", "r", encoding="utf-8") as f:
#             pull_request_comments = json.load(f)
            
#         create_repository_profile(repo_num, repo_id, repo_name, repo_language, contributor_list, commits, 
#                                   commit_comments, issues, issue_comments, df_issue_events, pull_requests,
#                                   pull_request_comments, df_pulls_events, df_watchers)   
# if __name__ == "__main__":
#     main()
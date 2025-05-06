""" This code filters contributors based on many criteia.
"""
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
import requests
import json
import time
import os
import pandas as pd

load_dotenv()

GITHUB_TOKEN = os.getenv("GIT_TOKEN3")

HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}"
}

def check_rate_limit():
    """
    Function that checks GitHub API limit and remaining number of responses
    """
    response = requests.get("https://api.github.com/rate_limit", headers=HEADERS)
    if response.status_code == 200:
        rate_limit = response.json()
        remaining = rate_limit["rate"]["remaining"]
        reset_time = rate_limit["rate"]["reset"]
        # print(f"\t\tRemaining Requests: {remaining}, Reset Time: {time.ctime(reset_time)}")
        return remaining, reset_time
    return 0, 0

# Retrieve user data
def get_user_data(url, user_id):
    """
    This function retrieves profile data for single user
    returns the first JSON result
    """
    max_retries = 2
    remaining, reset_time = check_rate_limit()
    if remaining == 0:
        wait_time = max(0, reset_time - time.time())
        print(f"\t\tRate limit exceeded! Waiting {wait_time:.2f} seconds before retrying...")
        time.sleep(wait_time + 1)
        
    retries = 1
    while retries <= max_retries:
        response = requests.get(url, headers=HEADERS)
        if response.status_code == 200:
            break  # Success, exit retry loop
        print(f"\tError collecting for {user_id}. Status Code: {response.status_code}. Retrying {retries}/{max_retries}...")
        time.sleep(5)  # Wait before retrying
        retries += 1
   
    if response.status_code != 200:
        print(f"\tError collecting for {user_id}. Status Code: {response.status_code}.")
        return {}
    
    data = response.json() # get one item 
    return data

def get_first_commit(user_id, commits):
    """
    Function gets a list of all commit
    Returns the earliest date
    """
    commit_dates = []
    for commit in commits:
        author = commit.get("author")
        date = commit.get("commit")
        if not author or not date:
            continue
        author = author["id"]
        date = date["author"]["date"]

        if author == user_id:
            commit_dates.append(date)
    return min(commit_dates) if commit_dates else None

def has_commit_in_year(user_id, start_date, end_date, commit_data):
    """
    Function finds instances of commits in first three years of contribution 
    """
    for commit in commit_data:
        author = commit.get("author")
        if author and author.get("id") == user_id:
            commit_date = pd.to_datetime(commit["commit"]["author"]["date"])
            if start_date <= commit_date < end_date:
                return "yes"
    return "no"

def process_contributors(contributors, repo_commits):
    """
    Function that updates contributors JSON 
    with created_date, registration_date, user_age, and LTC status.
    """
    current_year = 2025
    filtered_contributors = []
    
    for contributor in contributors:
        # Filter out contributors that are bots
        if contributor.get("type") == "Bot" or "bot" in contributor.get("login", "").lower():
            continue

        user_id = contributor["id"]
        user_data = get_user_data(contributor["url"], user_id)
        
        if not user_data: # no data found
            continue
        
        created_date = pd.to_datetime(user_data.get("created_at")) # day the account was created
        
        # Filter out if contributor doesn't have a create date
        if not created_date: 
            print("\tCould not get created date.")
            continue
        
        # Filter out if contributor doesn't have a registration date
        registration_date = pd.to_datetime(get_first_commit(user_id, repo_commits))

        if not registration_date:
            print("\tCould not get registration date.")
            continue
        
        registration_year = registration_date.year

        user_age = (registration_date - created_date).days
        
        # Filter out contributors who joined less than 3 years ago
        if registration_year > current_year - 3 or user_age < 0: # Can't computer if they're a LTC
            print("\tUser {user_id} joined too late.")
            continue

        # calculate if user has made any commits or pushes in three years
        one_year_date = pd.to_datetime(registration_date + relativedelta(years=1))
        two_year_date = pd.to_datetime(registration_date + relativedelta(years=2))
        three_year_date = pd.to_datetime(registration_date + relativedelta(years=3))
        four_year_date = pd.to_datetime(registration_date + relativedelta(years=4))
        
    
        one_year = has_commit_in_year(user_id, one_year_date, two_year_date, repo_commits)
        two_years = has_commit_in_year(user_id, two_year_date, three_year_date, repo_commits)
        three_years = has_commit_in_year(user_id, three_year_date, four_year_date, repo_commits)
        
        ltc = "yes" if one_year == "yes" and two_years == "yes" and three_years == "yes" else "no"
        
        contributor.update({
            "created_date": str(created_date),
            "registration_date": str(registration_date),
            "user_age": user_age,
            "one_year": one_year,
            "two_years": two_years,
            "three_years": three_years,
            "LTC": ltc
        })
        filtered_contributors.append(contributor)
        print(f"\tUser {user_id} saved!")
    return filtered_contributors

    
def main():
    save_path = "../FilteredContributors/"
    os.makedirs(save_path, exist_ok=True)

    # Open list of all repos 0-336 exists
    with open("../filteredRepos.json", 'r', encoding='utf-8') as f:
        repo_list = json.load(f)

    # Get contributor list
    for i, repo in enumerate(repo_list):
        # Check if the file already exists
        file_path = f"{save_path}contributors_{repo['name']}.json"
        
        if not os.path.exists(file_path):
            print(f"Getting data for repo {i}: {repo['name']}...")
            repo_directory = f"../Datasets/{repo['name']}/"
            with open(f"{repo_directory}contributors_{repo['name']}.json", "r", encoding='utf-8') as f:
                contributor_list = json.load(f)
        
            with open(f"{repo_directory}commits_{repo['name']}.json", "r", encoding='utf-8') as f:
                commits_list = json.load(f)
            
            filtered_contributors = process_contributors(contributor_list, commits_list)
            # If the file doesn't exist, write the filtered contributors
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(filtered_contributors, f, ensure_ascii=False, indent=4)
        else:
            print(f"File for {i}:{repo['name']} already exists, skipping.")
                
if __name__ == "__main__":
    # remaining, reset_time = check_rate_limit()
    main()
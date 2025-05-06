""" This code downloads repository data from Github API.
"""
from dotenv import load_dotenv
import requests
import json
import time
import re
import os

load_dotenv()

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN3")
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
        # print(f"Remaining Requests: {remaining}, Reset Time: {time.ctime(reset_time)}")
        return remaining, reset_time
    return 0, 0

def get_popular_repositories(min_stars=3000, per_page=100, max_repos=1000):
    """
    Function that fetches the top 1000 repositories 
    with more than 3000 stars using the GitHub API.
    """
    query = f"stars:>{min_stars} is:public"
    url = "https://api.github.com/search/repositories"
    all_repos = []
    page = 1
    while len(all_repos) < max_repos:
        params = {
            "q": query,
            "sort": "stars",
            "order": "desc",
            "per_page": per_page,
            "page": page
        }
        remaining, reset_time = check_rate_limit()
        if remaining == 0:
            wait_time = max(0, reset_time - time.time())
            print(f"\t\tRate limit exceeded! Waiting {wait_time:.2f} seconds before retrying...")
            time.sleep(wait_time + 1)
            continue
        response = requests.get(url, headers=HEADERS, params=params)
        if response.status_code != 200:
            print("Error retrieving repositories!")
            print(f"Response: {response.text}") 
            return all_repos
        repos = response.json().get("items", [])
        all_repos.extend(repos)
        if len(all_repos) >= max_repos or not repos:
            break
        page += 1
    return all_repos

def filter_github_repositories(repo_data):
    """
    Function that filters repositories based 
    on predefined criteria.
    """
    filtered_repos = [
        repo for repo in repo_data 
        if repo.get("language") 
        and repo.get("has_issues", True) 
        and not repo.get("fork", False) 
        and not repo.get("archived", False)
    ]
    with open("../filteredRepos.json", 'w', encoding='utf-8') as file:
        json.dump(filtered_repos, file, indent=4)
    print(f"Filtered data saved to filteredRepos.json with {len(filtered_repos)} repositories.")

def read_repo_data(repo, num):
    """
    Function that extracts and saves metadata 
    and URLs of interest for a given repository.
    """
    repo_name = repo["name"]
    owner = repo["owner"]["login"]
    repo_urls = {
        "contributors": repo["contributors_url"],
        "commits": repo["commits_url"],
        "commit_comments": repo["comments_url"],
        "issues": repo["issues_url"].replace("{/number}", "?state=all"),
        "issue_comments": repo["issue_comment_url"],
        "issue_events": repo["issue_events_url"],
        "pull_requests": repo["pulls_url"].replace("{/number}", "?state=all"),
        "pull_request_comments": f"https://api.github.com/repos/{owner}/{repo_name}/pulls/comments",
    }
    
    repo_directory = f"../Datasets/{repo_name}/"
    
    os.makedirs(repo_directory, exist_ok=True)

    print(f"Retrieving data for {num}: {repo_name}...")
    for data_type, url in repo_urls.items():
        save_to_JSON(data_type, url, repo_name, repo_directory)

def save_to_JSON(data_type, url, repo_name, repo_directory):
    """
    Function that fetches data from a GitHub API URL and saves it as JSON.
    """
    collected_data = []
    url = re.sub(r"\{.*?\}", "", url) 
    max_retries = 6
    
    print(f"\tFetching {data_type} data for {repo_name}...")
    while url:
        remaining, reset_time = check_rate_limit()
        if remaining == 0:
            wait_time = max(0, reset_time - time.time())
            print(f"\t\tRate limit exceeded! Waiting {wait_time:.2f} seconds before retrying...")
            time.sleep(wait_time + 1)
            continue
        
        retries = 1
        while retries <= max_retries:
            response = requests.get(url, headers=HEADERS)
            if response.status_code == 200:
                break  # Success, exit retry loop
            print(f"Error collecting {data_type} for {repo_name}. Status Code: {response.status_code}. Retrying {retries}/{max_retries}...")
            time.sleep(5)  # Wait before retrying
            retries += 1

        if response.status_code != 200:
            print(f"Error collecting {data_type} for {repo_name}. Status Code: {response.status_code}")
            break
        
        data = response.json()
        collected_data.extend(data)
        url = response.links.get('next', {}).get('url')
       
    save_progress(repo_directory, repo_name, data_type, collected_data)
    print(f"\tSaved {len(collected_data)} records for {data_type}.")

def save_progress(directory, repo_name, data_type, data):
    """
    Function that saves the fetched data to a JSON file.
    """
    file_path = os.path.join(directory, f"{data_type}_{repo_name}.json")
    with open(file_path, 'w', encoding='utf-8') as file:
        json.dump(data, file, indent=4) 

def main():
    # popularRepos = get_popular_repositories() # get top repos
    # filter_github_repositories(popularRepos)  # filter github repos
    
    with open('../filteredRepos.json', 'r', encoding='utf-8') as file:
        filtered_repos = json.load(file)
        
    for i, repo in enumerate(filtered_repos):
        read_repo_data(repo, i)

if __name__ == "__main__":
    # remaining, reset_time = check_rate_limit()
    main()

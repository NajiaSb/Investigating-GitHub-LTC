""" This code collects comments and performs sentiment analysis
    It also merges all csvs in each directory into one.
"""
import pandas as pd
import json
import re
import os
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
from nltk.sentiment import SentimentIntensityAnalyzer
from dateutil.relativedelta import relativedelta
from langdetect import detect
import unicodedata

# Ensure required NLTK data is downloaded
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('wordnet')
nltk.download('omw-1.4')
nltk.download('vader_lexicon')

stop_words = set(stopwords.words('english'))
lemmatizer = WordNetLemmatizer()
sia = SentimentIntensityAnalyzer()

MAX_GHTORRENT_DATE = pd.to_datetime("2021-03-06 23:57:37+00:00")  # Max date from GHTorrent data

def clean_text(text):
    try:
        if detect(text) != 'en':
            return None
    except:  # noqa: E722
        return None

    # Normalize text
    text = unicodedata.normalize("NFKD", text)
    text = re.sub(r'\s+', ' ', text).strip()

    # Remove code snippets in backticks or triple backticks
    text = re.sub(r'```.*?```', '', text, flags=re.DOTALL)
    text = re.sub(r'`[^`]*`', '', text)

    # Remove URLs
    text = re.sub(r'\(?(https?:\/\/[^\s)]+)\)?', '', text)
    text = re.sub(r'www\.[^\s]+', '', text)

    # Remove @mentions
    text = re.sub(r'@\w+', '', text)

    # Remove all non-word characters (keep spaces and words only)
    text = re.sub(r'[^\w\s]', '', text)

    # Tokenize, remove stopwords and lemmatize
    tokens = word_tokenize(text.lower())
    cleaned_tokens = [lemmatizer.lemmatize(word) for word in tokens if word.isalpha() and word not in stop_words]

    if len(cleaned_tokens) < 2:
        return None

    return ' '.join(cleaned_tokens)

def calculate_sentiment(text):
    """
    Perform sentiment analysis using VADER.
    Returns: 1 for positive, -1 for negative, 0 for neutral.
    """
    scores = sia.polarity_scores(text)
    compound = scores['compound']
    
    if compound >= 0.05:
        return 1
    elif compound <= -0.05:
        return -1
    else:
        return 0

def save_commit_comments(repo_list):
    output_path = "../Tables/Sentiment/commit_comments.csv"
    all_rows = []

    for i, repo in enumerate(repo_list): 
        repo_name = repo["name"]
        repo_id = repo["id"]
        commit_path = f"../Datasets/{repo_name}/commits_{repo_name}.json"
        comment_path = f"../Datasets/{repo_name}/commit_comments_{repo_name}.json"

        if not os.path.exists(commit_path) or not os.path.exists(comment_path):
            continue

        print(f"Processing repo commits {i}:{repo_name}...")

        with open(commit_path, "r", encoding="utf-8") as f:
            commits = json.load(f)
        with open(comment_path, "r", encoding="utf-8") as f:
            commit_comments = json.load(f)

        commit_user_map = {commit["sha"]: (commit.get("author") or {}).get("id", "") for commit in commits}

        for comment in commit_comments:
            commit_id = comment.get("commit_id")
            comment_body = comment.get("body", "")
            date = comment.get("created_at")
            original_user = commit_user_map.get(commit_id, "")

            comment = clean_text(comment_body)

            if commit_id and original_user and comment:
                all_rows.append({
                    "repo_id": repo_id,
                    "contributor": original_user,
                    "date":date,
                    "polarity": calculate_sentiment(comment),
                    "comment": comment,
                })

    append_df_to_csv(output_path, all_rows)

def save_pull_comments(repo_list):
    output_path = "../Tables/Sentiment/pull_comments.csv"
    all_rows = []

    for i, repo in enumerate(repo_list): 
        repo_name = repo["name"]
        repo_id = repo["id"]
        pr_path = f"../Datasets/{repo_name}/pull_requests_{repo_name}.json"
        comments_path = f"../Datasets/{repo_name}/pull_request_comments_{repo_name}.json"

        if not os.path.exists(pr_path) or not os.path.exists(comments_path):
            continue

        print(f"Processing repo pull {i}:{repo_name}...")

        with open(pr_path, "r", encoding="utf-8") as f:
            pull_requests = json.load(f)
        with open(comments_path, "r", encoding="utf-8") as f:
            pull_request_comments = json.load(f)

        pr_user_map = {pr["head"]["sha"]: pr["user"]["id"] for pr in pull_requests if "user" in pr}

        for comment in pull_request_comments:
            pull_id = comment.get("commit_id")
            comment_body = comment.get("body", "")
            original_user = pr_user_map.get(pull_id, "")
            date = comment.get("created_at")
            comment = clean_text(comment_body)

            if pull_id and original_user and comment:
                all_rows.append({
                    "repo_id": repo_id,
                    "contributor": original_user,
                    "date": date,
                    "polarity": calculate_sentiment(comment),
                    "comment": comment,
                })

    append_df_to_csv(output_path, all_rows)

def save_issue_comments(repo_list):
    output_path = "../Tables/Sentiment/issue_comments.csv"
    all_rows = []

    for i, repo in enumerate(repo_list): 
        repo_name = repo["name"]
        repo_id = repo["id"]
        issue_path = f"../Datasets/{repo_name}/issues_{repo_name}.json"
        comment_path = f"../Datasets/{repo_name}/issue_comments_{repo_name}.json"

        if not os.path.exists(issue_path) or not os.path.exists(comment_path):
            continue

        print(f"Processing repo issue {i}:{repo_name}...")

        with open(issue_path, "r", encoding="utf-8") as f:
            issues = json.load(f)
        with open(comment_path, "r", encoding="utf-8") as f:
            issue_comments = json.load(f)

        issue_user_map = {issue["url"]: issue["user"]["id"] for issue in issues if "user" in issue}

        for comment in issue_comments:
            issue_url = comment.get("issue_url")
            comment_body = comment.get("body", "")
            original_user = issue_user_map.get(issue_url, "")
            date = comment.get("created_at")
            comment = clean_text(comment_body)

            if issue_url and original_user and comment:
                all_rows.append({
                    "repo_id": repo_id,
                    "contributor": original_user,
                    "date":date,
                    "polarity": calculate_sentiment(comment),
                    "comment": comment,
                })

    append_df_to_csv(output_path, all_rows)

def append_df_to_csv(path, rows):
    if not rows:
        return

    os.makedirs(os.path.dirname(path), exist_ok=True)
    df = pd.DataFrame(rows)

    write_header = not os.path.isfile(path)
    df.to_csv(path, mode='a', header=write_header, index=False, encoding='utf-8')
    
def construct_tables(repo_list):
    output_path = "../Tables/Sentiment/comments.csv"

    commit_comments = pd.read_csv("../Tables/Sentiment/commit_comments.csv")
    pull_comments = pd.read_csv("../Tables/Sentiment/pull_comments.csv")
    issue_comments = pd.read_csv("../Tables/Sentiment/issue_comments.csv")
    
    commit_comments["date"] = pd.to_datetime(commit_comments["date"], utc=True, errors="coerce")
    pull_comments["date"] = pd.to_datetime(pull_comments["date"], utc=True, errors="coerce")
    issue_comments["date"] = pd.to_datetime(issue_comments["date"], utc=True, errors="coerce")


    all_rows = []

    for i, repo in enumerate(repo_list): 
        repo_name = repo["name"]
        repo_id = repo["id"]
        contributor_path = f"../FilteredContributors/contributors_{repo_name}.json"
        
        print(f"Processing repo issue {i}:{repo_name}...")
        
        if not os.path.exists(contributor_path):
            continue

        with open(contributor_path, "r", encoding="utf-8") as f:
            contributors = json.load(f)
        
        for c in contributors:
            contributor_id = c.get("id")
            contributor_date = pd.to_datetime(c.get("created_date"))
            one_year_date = contributor_date + relativedelta(years=1)
        
            ltc_1 = 1 if c["one_year"] == "yes" else 0
            ltc_2 = 1 if c["one_year"] == "yes" and c["two_years"] == "yes" else 0
            ltc_3 = 1 if c["LTC"] == "yes" else 0

            # Filter comments from all sources
            def filter_comments(df):
                df = df[(df["repo_id"] == repo_id) & (df["contributor"] == contributor_id)].copy()
                return df[df["date"] < one_year_date]


            user_commit_comments = filter_comments(commit_comments)
            user_pull_comments = filter_comments(pull_comments)
            user_issue_comments = filter_comments(issue_comments)

            all_comments = pd.concat([user_commit_comments, user_pull_comments, user_issue_comments], ignore_index=True)

            num_comments = len(all_comments)

            if num_comments != 0:
                avg_sentiment = round(all_comments["polarity"].mean(), 1) if num_comments > 0 else 0
                all_rows.append({
                    "repo_name": repo_name,
                    "repo_id": repo_id,
                    "contributor_id": contributor_id,
                    "date": contributor_date,
                    "num_comments": num_comments,
                    "avg_sentiment": avg_sentiment,
                    "ltc_1": ltc_1,
                    "ltc_2": ltc_2,
                    "ltc_3": ltc_3
                })

    if all_rows:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df = pd.DataFrame(all_rows)
        df.to_csv(output_path, index=False, encoding="utf-8")
        
def merge_csvs():
    # Directories to merge CSV files within
    directories = {
        "RepositoryProfiles": "../Tables/RepositoryProfiles",
        "RepositoryMonthlyActivity": "../Tables/RepositoryMonthlyActivity",
        "DeveloperMonthlyActivity": "../Tables/DeveloperMonthlyActivity",
        "DeveloperProfiles": "../Tables/DeveloperProfiles"
    }

    # Iterate over each directory
    for dir_name, directory in directories.items():
        dfs = []
        
        # Iterate over all files in the directory
        for filename in os.listdir(directory):
            if filename.endswith(".csv"):
                file_path = os.path.join(directory, filename)
                df = pd.read_csv(file_path)
                dfs.append(df)
        
        # If there are CSV files in the directory, merge them
        if dfs:
            merged_df = pd.concat(dfs, ignore_index=True)
            # Define the merged filename based on the directory name
            merged_filename = os.path.join(directory, f"{dir_name}.csv")
            # Write the merged DataFrame to a new CSV file
            merged_df.to_csv(merged_filename, index=False)
            
            print(f"CSV files in {directory} have been successfully merged into '{dir_name}.csv'.")
        else:
            print(f"No CSV files found in {directory}.")

def main():
    with open('../filteredRepos.json', 'r', encoding='utf-8') as f:
        repo_list = json.load(f)
    
    # Get all comments and clean them
    save_commit_comments(repo_list)
    save_issue_comments(repo_list)
    save_pull_comments(repo_list)
    
    # Construct sentiment feature tables
    construct_tables(repo_list)
    
    # Merge four feature tables
    merge_csvs()

if __name__ == "__main__":
    main()

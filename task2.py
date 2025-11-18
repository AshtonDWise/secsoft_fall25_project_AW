import pandas as pd
from datasets import load_dataset
import re # Needed for Task-4 and Task-5


# Load all required datasets
df_pr = load_dataset("hao-li/AIDev", "all_pull_request")
df_repo = load_dataset("hao-li/AIDev", "all_repository")
df_task = load_dataset("hao-li/AIDev", "pr_task_type")
df_commit = load_dataset("hao-li/AIDev", "pr_commit_details")

# Convert each to pandas DataFrame
df_pr_pd = df_pr["train"].to_pandas()
df_repo_pd = df_repo["train"].to_pandas()
df_task_pd = df_task["train"].to_pandas()
df_commit_pd = df_commit["train"].to_pandas()

# Task-2: Create the all_repository CSV
df_task2 = df_repo_pd[['id', 'language', 'stars', 'url']].copy()
df_task2.columns = ['REPOID', 'LANG', 'STARS', 'REPOURL']


# Save to CSV
df_task2.to_csv('task2_repositories.csv', index=False)
print("Task-2: 'task2_repositories.csv' created.")
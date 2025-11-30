import pandas as pd
from datasets import load_dataset
import re  # Needed for Task-4 and Task-5


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


# Helper to clean the diff / patch text so that we do not run into
# encoding issues when saving the CSV file.
def clean_diff(text: str) -> str:
    if pd.isna(text):
        return ""
    # Ensure we work with a string
    text = str(text)
    # Replace newlines and carriage-returns with spaces
    text = text.replace("\r", " ").replace("\n", " ")
    # Drop non-ASCII characters that can cause encoding errors
    text = re.sub(r"[^\x00-\x7F]+", " ", text)
    # Collapse runs of whitespace into a single space
    text = re.sub(r"\s+", " ", text).strip()
    return text


# Build the Task-4 dataframe from pr_commit_details
df_task4 = df_commit_pd[
    [
        "pr_id",
        "sha",
        "message",
        "filename",
        "status",
        "additions",
        "deletions",
        "changes",
        "patch",
    ]
].copy()

df_task4.columns = [
    "PRID",
    "PRSHA",
    "PRCOMMITMESSAGE",
    "PRFILE",
    "PRSTATUS",
    "PRADDS",
    "PRDELSS",
    "PRCHANGECOUNT",
    "PRDIFF",
]

# Clean the PRDIFF text
df_task4["PRDIFF"] = df_task4["PRDIFF"].apply(clean_diff)

# Save to CSV
df_task4.to_csv("task4.csv", index=False)
print("Task-4: 'task4.csv' created.")
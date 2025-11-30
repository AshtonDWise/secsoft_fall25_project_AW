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


# --------------------------------------------------------------------
# Build the intermediate dataframes that correspond to Tasks 1, 3, and 4
# (Task-2 is not needed for Task-5).
# --------------------------------------------------------------------

# Task-1 style dataframe (PR meta-data)
df_task1 = df_pr_pd[["title", "id", "agent", "body"]].copy()
df_task1.columns = ["TITLE", "ID", "AGENT", "BODYSTRING"]

# Task-3 style dataframe (task type and confidence)
df_task3 = df_task_pd[["id", "title", "reason", "type", "confidence"]].copy()
df_task3.columns = ["PRID", "PRTITLE", "PRREASON", "PRTYPE", "CONFIDENCE"]

# Task-4 style dataframe (commit details, subset needed for security scan)
df_task4 = df_commit_pd[["pr_id", "message", "filename", "patch"]].copy()
df_task4.columns = ["PRID", "PRCOMMITMESSAGE", "PRFILE", "PRDIFF"]


# Reuse the same cleaning for the diff / patch field that we used in Task-4
def clean_diff(text: str) -> str:
    if pd.isna(text):
        return ""
    text = str(text)
    text = text.replace("\r", " ").replace("\n", " ")
    text = re.sub(r"[^\x00-\x7F]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


df_task4["PRDIFF"] = df_task4["PRDIFF"].apply(clean_diff)

# Aggregate commit-level text per pull request
df_task4["TEXT_COMMIT"] = (
    df_task4[["PRCOMMITMESSAGE", "PRFILE", "PRDIFF"]]
    .astype(str)
    .agg(" ".join, axis=1)
)

df_commit_agg = (
    df_task4.groupby("PRID")["TEXT_COMMIT"]
    .apply(lambda x: " ".join(x))
    .reset_index()
)

# --------------------------------------------------------------------
# Join everything together
# --------------------------------------------------------------------

# Join Task-1 (ID/AGENT) with Task-3 (TYPE/CONFIDENCE and other PR text)
df_merged = df_task1.merge(
    df_task3,
    left_on="ID",
    right_on="PRID",
    how="left",
)

# Join in aggregated commit text (from Task-4)
df_merged = df_merged.merge(
    df_commit_agg,
    left_on="ID",
    right_on="PRID",
    how="left",
    suffixes=("", "_COMMIT"),
)

# Collect all textual fields we want to inspect for security-related keywords
text_columns = ["TITLE", "BODYSTRING", "PRTITLE", "PRREASON", "TEXT_COMMIT"]

for col in text_columns:
    if col not in df_merged.columns:
        df_merged[col] = ""

df_merged["FULLTEXT"] = (
    df_merged[text_columns]
        .fillna("")
        .astype(str)
        .agg(" ".join, axis=1)
)

# --------------------------------------------------------------------
# SECURITY flag based on the keyword list provided in the README
# --------------------------------------------------------------------

security_keywords = [
    "race",
    "racy",
    "buffer",
    "overflow",
    "stack",
    "integer",
    "signedness",
    "underflow",
    "improper",
    "unauthenticated",
    "gain access",
    "permission",
    "cross site",
    "css",
    "xss",
    "denial service",
    "dos",
    "crash",
    "deadlock",
    "injection",
    "request forgery",
    "csrf",
    "xsrf",
    "forged",
    "security",
    "vulnerability",
    "vulnerable",
    "exploit",
    "attack",
    "bypass",
    "backdoor",
    "threat",
    "expose",
    "breach",
    "violate",
    "fatal",
    "blacklist",
    "overrun",
    "insecure",
]

security_pattern = re.compile(
    "|".join(re.escape(k) for k in security_keywords),
    re.IGNORECASE,
)


def has_security_keyword(text: str) -> int:
    if not isinstance(text, str):
        text = "" if pd.isna(text) else str(text)
    return 1 if security_pattern.search(text) else 0


df_merged["SECURITY"] = df_merged["FULLTEXT"].apply(has_security_keyword)

# --------------------------------------------------------------------
# Build the final Task-5 dataframe and save to CSV
# --------------------------------------------------------------------

df_task5 = df_merged[["ID", "AGENT", "PRTYPE", "CONFIDENCE", "SECURITY"]].copy()
df_task5.columns = ["ID", "AGENT", "TYPE", "CONFIDENCE", "SECURITY"]

df_task5.to_csv("task5.csv", index=False)
print("Task-5: 'task5.csv' created.")
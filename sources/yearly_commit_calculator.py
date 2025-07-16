from asyncio import sleep
from json import dumps
from re import search
from datetime import datetime
from typing import Dict, Tuple

from manager_download import DownloadManager as DM
from manager_environment import EnvironmentManager as EM
from manager_github import GitHubManager as GHM
from manager_file import FileManager as FM
from manager_debug import DebugManager as DBM


async def calculate_commit_data(repositories: Dict) -> Tuple[Dict, Dict]:
    """
    Calculate commit data by years.
    Commit data includes contribution additions and deletions in each quarter of each recorded year.

    :param repositories: user repositories info dictionary.
    :returns: Commit quarter yearly data dictionary.
    """
    DBM.i("Calculating commit data...")
    if EM.DEBUG_RUN:
        content = FM.cache_binary("commits_data.pick", assets=True)
        if content is not None:
            DBM.g("Commit data restored from cache!")
            return tuple(content)
        else:
            DBM.w("No cached commit data found, recalculating...")

    yearly_data = dict()
    date_data = dict()
    for ind, repo in enumerate(repositories):
        if repo["name"] not in EM.IGNORED_REPOS:
            repo_name = "[private]" if repo["isPrivate"] else f"{repo['owner']['login']}/{repo['name']}"
            DBM.i(f"\t{ind + 1}/{len(repositories)} Retrieving repo: {repo_name}")
            await update_data_with_commit_stats(repo, yearly_data, date_data)
    DBM.g("Commit data calculated!")

    if EM.DEBUG_RUN:
        FM.cache_binary("commits_data.pick", [yearly_data, date_data], assets=True)
        FM.write_file("commits_data.json", dumps([yearly_data, date_data]), assets=True)
        DBM.g("Commit data saved to cache!")
    return yearly_data, date_data


async def update_data_with_commit_stats(repo_details: Dict, yearly_data: Dict, date_data: Dict):
    """
    Updates yearly commit data with commits from given repository.
    Skips update if the commit isn't related to any repository.

    :param repo_details: Dictionary with information about the given repository.
    :param yearly_data: Yearly data dictionary to update.
    :param date_data: Commit date dictionary to update.
    """
    owner = repo_details["owner"]["login"]
    branch_data = await DM.get_remote_graphql("repo_branch_list", owner=owner, name=repo_details["name"])
    if len(branch_data) == 0:
        DBM.w("\t\tSkipping repo.")
        return

    total_commits = 0
    total_additions = 0
    total_deletions = 0
    processed_commits = set()  # Track processed commit OIDs to avoid duplicates
    for branch in branch_data:
        commit_data = await DM.get_remote_graphql("repo_commit_list", owner=owner, name=repo_details["name"], branch=branch["name"], id=GHM.USER.node_id)

        for commit in commit_data:
            # Skip if this commit has already been processed (avoid duplicates across branches)
            commit_oid = commit["oid"]
            if commit_oid in processed_commits:
                continue
            processed_commits.add(commit_oid)

            match = search(r"\d+-\d+-\d+", commit["committedDate"])
            if match is None:
                continue
            date = match.group()
            curr_year = datetime.fromisoformat(date).year
            quarter = (datetime.fromisoformat(date).month - 1) // 3 + 1

            # Verify author is the current user
            author_login = commit.get("author", {}).get("user", {}).get("login", "Unknown")
            if author_login != GHM.USER.login and author_login != "Unknown":
                DBM.w(f"Skipping commit by {author_login} in repo {repo_details['name']} (not current user)")
                continue

            total_commits += 1
            total_additions += commit["additions"]
            total_deletions += commit["deletions"]

            if repo_details["name"] not in date_data:
                date_data[repo_details["name"]] = dict()
            if branch["name"] not in date_data[repo_details["name"]]:
                date_data[repo_details["name"]][branch["name"]] = dict()
            date_data[repo_details["name"]][branch["name"]][commit["oid"]] = commit["committedDate"]

            if repo_details["primaryLanguage"] is not None:
                if curr_year not in yearly_data:
                    yearly_data[curr_year] = dict()
                if quarter not in yearly_data[curr_year]:
                    yearly_data[curr_year][quarter] = dict()
                if repo_details["primaryLanguage"]["name"] not in yearly_data[curr_year][quarter]:
                    yearly_data[curr_year][quarter][repo_details["primaryLanguage"]["name"]] = {"add": 0, "del": 0}
                yearly_data[curr_year][quarter][repo_details["primaryLanguage"]["name"]]["add"] += commit["additions"]
                yearly_data[curr_year][quarter][repo_details["primaryLanguage"]["name"]]["del"] += commit["deletions"]

        if not EM.DEBUG_RUN:
            await sleep(0.4)

    # repo_name = f"{owner}/{repo_details['name']}"
    # print(f"Repo {repo_name} has {total_commits} commits, total additions: {total_additions}, total deletions: {total_deletions}")

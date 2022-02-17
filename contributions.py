import requests
import yaml
import json
import asyncio
import aiohttp
import sys
import argparse
import os
from pathlib import Path
import datetime
import re
import textwrap
from typing import *


def j(o):
    print(json.dumps(o, indent=2))


def sprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


async def ngather(n, *tasks):
    semaphore = asyncio.Semaphore(n)

    async def sem_task(task):
        async with semaphore:
            return await task

    return await asyncio.gather(*(sem_task(task) for task in tasks))


class Contributions:
    def __init__(
        self,
        login: str,
        date_from: str,
        date_to: str,
        repos: List[str],
        orgs: List[str],
        gql: "GraphQL",
    ):
        self.login = login
        self.gql = gql
        self.date_to = date_to
        self.date_from = date_from
        self.orgs = orgs
        self.repos = set(repos)

    async def query(self, query: str, variables: Dict[str, str] = None):
        if variables is None:
            variables = {}

        variables["from"] = self.date_from.isoformat()
        variables["to"] = self.date_to.isoformat()
        return await self.gql.query(query, variables=variables, retries=1)

    async def per_orgs(self, field: str, fields: str, aggregate):
        org_queries = ""
        org_vars = ", ".join([f"$organization{i}: ID" for i in range(len(self.orgs))])
        for i, org in enumerate(self.orgs):
            org_queries += f"""
                a{i}:contributionsCollection(from: $from, to: $to, organizationID: $organization{i}) {{
                    {field} {{
                    repository {{
                        nameWithOwner
                    }}
                    contributions(first: 100, after: $cursor) {{
                        {fields}
                    }}
                    }}
                }}
            """
        query = f"""
          query ($login: String!, $from: DateTime!, $to: DateTime!, $cursor: String, {org_vars}) {{
            user(login: $login) {{
                {org_queries}
            }}
          }}
        """
        variables = {"login": self.login}
        for i, org in enumerate(self.orgs):
            variables[f"organization{i}"] = org
        r = await self.query(query, variables=variables)

        for i in range(len(self.orgs)):
            contributions = r["data"]["user"][f"a{i}"][field]
            for contribution in contributions:
                if contribution["repository"]["nameWithOwner"] in self.repos:
                    aggregate(
                        contribution["repository"]["nameWithOwner"],
                        contribution["contributions"],
                    )

    async def run_per_org(self, coro):
        coros = [coro(org) for org in self.orgs]
        await asyncio.gather(*coros)

    async def commits(self):
        results = []

        def aggregate(repo, data):
            results.append((repo, data))

        await self.per_orgs(
            field="commitContributionsByRepository",
            fields="totalCount",
            aggregate=aggregate,
        )
        return results

    async def issues_created(self):
        results = []

        def aggregate(repo, data):
            nonlocal results
            results += [pr["issue"] for pr in data["nodes"]]

        await self.per_orgs(
            field="issueContributionsByRepository",
            fields="""
                nodes {
                    issue {
                        title
                        url
                    }
                }
            """,
            aggregate=aggregate,
        )
        return results

    async def prs_created(self):
        results = []

        def aggregate(repo, data):
            nonlocal results
            results += [pr["pullRequest"] for pr in data["nodes"]]

        await self.per_orgs(
            field="pullRequestContributionsByRepository",
            fields="""
                nodes {
                    pullRequest {
                        title
                        changedFiles
                        deletions
                        additions
                        url
                    }
                }
            """,
            aggregate=aggregate,
        )
        return results

    async def pr_reviews(self):
        results = []

        def aggregate(repo, data):
            nonlocal results
            results += [pr["pullRequestReview"] for pr in data["nodes"]]

        await self.per_orgs(
            field="pullRequestReviewContributionsByRepository",
            fields="""
                nodes {
                    pullRequestReview {
                        url
                    }
                }
            """,
            aggregate=aggregate,
        )
        return results

    async def issue_and_pr_comments(self):
        # TODO: Not paged but who has more than 100 comments anyways...
        query = """
        query($login:String!){
            user(login:$login){
                issueComments(last:100) {
                nodes {
                    url
                    updatedAt
                    repository { nameWithOwner }
                }
                }
            }
        }
        """
        results = []
        r = await self.gql.query(query, variables={"login": self.login})
        for item in r["data"]["user"]["issueComments"]["nodes"]:
            date = datetime.datetime.strptime(item["updatedAt"], "%Y-%m-%dT%H:%M:%SZ")
            if self.date_from > date or date > self.date_to:
                continue
            if item["repository"]["nameWithOwner"] not in self.repos:
                continue
            results.append({"url": item["url"]})
        return results

    async def discourse(self):
        base_url = "https://discuss.tvm.apache.org/search.json?"
        after = self.date_from.strftime("%Y-%m-%d")
        if datetime.datetime.now() - self.date_to > datetime.timedelta(days=1):
            raise RuntimeError(
                "Cannot fetch discourse results in a range of dates, only after or before a single date"
            )

        query = f"q=%40{self.login}%20after%3A{after}"
        url = base_url + query
        base = "https://discuss.tvm.apache.org/t/"
        # TODO: Auth doesn't work??
        # headers = {
        #     "Api-Key": os.environ["DISCOURSE_API_KEY"],
        #     "Api-Username": os.environ["DISCOURSE_USERNAME"],
        # }

        async def try_fetch(retries=5, backoff=0):
            if retries == 0:
                raise RuntimeError("Failed to fetch discuss data")

            await asyncio.sleep(backoff)
            async with self.gql.session.get(url) as r:
                r = await r.json()
                j(r)
                error = r["grouped_search_result"]["error"]
                if error is not None:
                    sprint(f"Failed to fetch, retrying {retries}")
                    return await try_fetch(retries=retries - 1, backoff=backoff + 30)
                else:
                    posts = r.get("posts", [])
                    posts = [
                        {"url": base + f'{post["topic_id"]}/{post["id"]}'}
                        for post in posts
                    ]
                    topics = r.get("topics", [])
                    topics = [{"url": base + f'{topic["id"]}'} for topic in topics]
                    return posts, topics

        posts, topics = await try_fetch()
        return posts, topics


class GraphQL:
    def __init__(self, session: aiohttp.ClientSession) -> None:
        self.session = session

    def log_rate_limit(self, headers: Any) -> None:
        remaining = headers.get("X-RateLimit-Remaining")
        used = headers.get("X-RateLimit-Used")
        total = headers.get("X-RateLimit-Limit")
        reset_timestamp = int(headers.get("X-RateLimit-Reset", 0))  # type: ignore
        reset = datetime.datetime.fromtimestamp(reset_timestamp).strftime(
            "%a, %d %b %Y %H:%M:%S"
        )

        sprint(
            f"[rate limit] Used {used}, {remaining} / {total} remaining, reset at {reset}"
        )

    def compress_query(self, query: str) -> str:
        query = query.replace("\n", "")
        query = re.sub("\s+", " ", query)
        return query

    async def query(
        self,
        query: str,
        variables: Dict[str, str] = None,
        verify: Optional[Callable[[Any], None]] = None,
        retries: int = 5,
    ) -> Any:
        """
        Run an authenticated GraphQL query
        """
        # Remove unnecessary white space
        query = self.compress_query(query)
        if retries <= 0:
            raise RuntimeError(f"Query {query[:100]} failed, no retries left")

        if variables is None:
            variables = {}
        url = "https://api.github.com/graphql"
        if os.getenv("DEBUG", "") == "1":
            sprint({"query": query, "variables": variables})
        try:
            async with self.session.post(
                url, json={"query": query, "variables": variables}
            ) as resp:
                self.log_rate_limit(resp.headers)
                r = await resp.json()
            if "data" not in r:
                raise RuntimeError(json.dumps(r, indent=2))
            if verify is not None:
                verify(r)
            return r
        except Exception as e:
            sprint(
                f"Retrying query {query[:100]}, remaining attempts: {retries - 1}\n{e}",
                file=sys.stderr,
            )
            return await self.query(query, verify=verify, retries=retries - 1)

    async def triagers(self):
        async with self.session.get(
            "https://raw.githubusercontent.com/apache/tvm/main/.asf.yaml"
        ) as r:
            return yaml.safe_load(await r.text())["github"]["collaborators"]

    async def organizations(self, orgs: List[str]):
        """
        find org ids by name
        """
        query = "{"
        for i, name in enumerate(orgs):
            query += f'a{i}:organization(login:"{name}") {{ id }}\n'

        query += "}"

        r = await self.query(query)

        result = {}
        for i, data in enumerate(r["data"].values()):
            result[orgs[i]] = data["id"]
        return result

    async def paged_fetch(self, query, data_path, variables=None):
        page_info_path = f"data.{data_path}.pageInfo"
        page_info_path = page_info_path.split(".")
        data_path = f"data.{data_path}.nodes".split(".")
        result = []
        cursor = None
        if variables is None:
            variables = {}

        while True:
            variables["cursor"] = cursor
            r = await self.query(query, variables=variables, retries=1)
            page_info = r
            for key in page_info_path:
                page_info = page_info[key]

            data = r
            for key in data_path:
                data = data[key]

            result += data
            cursor = page_info["endCursor"]
            next_page = page_info["hasNextPage"]
            if not next_page:
                break

        return result

    async def fetch_committers(self):
        triagers = await self.triagers()
        query = """query($cursor:String){
        repository(name: "tvm", owner: "apache") {
            assignableUsers(first:100, after: $cursor) {
                totalCount
                pageInfo {
                    endCursor
                    hasNextPage
                }
                nodes {
                    login
                }
            }
        }
        }"""

        users = await self.paged_fetch(query, data_path="repository.assignableUsers")
        users = [user["login"] for user in users]
        users = [user for user in users if user not in set(triagers)]
        return users

    async def fetch_contributors(self):
        query = """query($cursor:String){
        repository(name: "tvm", owner: "apache") {
            mentionableUsers(first:100, after: $cursor) {
                totalCount
                pageInfo {
                    endCursor
                    hasNextPage
                }
                nodes {
                    login
                }
            }
        }
        }"""

        users = await self.paged_fetch(query, data_path="repository.mentionableUsers")
        users = [user["login"] for user in users]
        return users

    async def find_contributions_for_user(self, cont, login: str, cache: str):
        with open(cache, "r") as f:
            contents = json.load(f)

        # await cont.discourse()
        # exit(0)
        if login in contents:
            return True

        (
            issues_created,
            commits,
            prs,
            pr_reviews,
            issue_and_pr_comments,
        ) = await asyncio.gather(
            cont.issues_created(),
            cont.commits(),
            cont.prs_created(),
            cont.pr_reviews(),
            cont.issue_and_pr_comments(),
        )

        commit_count = sum(x[1]["totalCount"] for x in commits)
        partial_total = (
            len(issues_created)
            + len(prs)
            + commit_count
            + len(pr_reviews)
            + len(issue_and_pr_comments)
        )

        # API is expensive, don't hit it unless there are other indicators
        if partial_total > 2:
            posts, participated_topics = await cont.discourse()
        else:
            posts, participated_topics = [], []

        # Re-open after yielding with 'await'
        with open(cache, "r") as f:
            contents_out = json.load(f)

        contents_out[login] = {
            "prs": prs,
            "pr_reviews": pr_reviews,
            "issues_created": issues_created,
            "commits": commits,
            "issue_and_pr_comments": issue_and_pr_comments,
            "posts": posts,
            "participated_topics": participated_topics,
        }

        with open(cache, "w") as f:
            json.dump(contents_out, f)

        print(f"Successfully fetched contributions for https://github.com/{login}")
        return False


async def main(args):
    repos = [r.strip() for r in args.repos.split(",")]
    orgs = args.organizations.split(",")
    date_from = args.date_from
    date_to = args.date_to
    now = datetime.datetime.now().replace(microsecond=0)
    if date_to is None:
        date_to = now
    if date_from is None:
        date_from = date_to - datetime.timedelta(weeks=4)

    cache_path = Path(args.cache)
    if not cache_path.exists():
        with open(cache_path, "w") as f:
            f.write("{}")

    with open(cache_path) as f:
        contents = json.load(f)

    contents["$$FROM$$"] = date_from.isoformat()
    contents["$$TO$$"] = date_to.isoformat()
    contents["$$REPOS$$"] = repos
    contents["$$ORGS$$"] = orgs

    with open(cache_path, "w") as f:
        json.dump(contents, f)

    async with aiohttp.ClientSession(
        headers={
            "Authorization": "token {}".format(os.environ["GITHUB_TOKEN"]),
            "Accept": "application/vnd.github.machine-man-preview+json",
        }
    ) as aiosession:
        gql = GraphQL(aiosession)
        org_ids = await gql.organizations([o.strip() for o in orgs])
        org_ids = list(org_ids.values())

        if len(os.getenv("DEBUG", "")) > 3:
            committer_candidates = [os.getenv("DEBUG", "")]
        else:
            with open(cache_path) as f:
                contents = json.load(f)

            if "$$CANDIDATES$$" in contents:
                committer_candidates = contents["$$CANDIDATES$$"]
            else:
                existing_committers = set(await gql.fetch_committers())
                sprint(f"Gathering potential committers")
                committer_candidates = set(await gql.fetch_contributors())
                committer_candidates = [
                    user
                    for user in committer_candidates
                    if user not in existing_committers
                ]
                with open(cache_path) as f:
                    contents = json.load(f)
                contents["$$CANDIDATES$$"] = committer_candidates
                with open(cache_path, "w") as f:
                    json.dump(contents, f)

        committer_candidates = sorted(committer_candidates)
        n = len(committer_candidates)
        failures = []
        hits = []
        misses = []

        async def check(i, user):
            cont = Contributions(
                user,
                repos=repos,
                orgs=org_ids,
                date_from=date_from,
                date_to=date_to,
                gql=gql,
            )
            sprint(f"Checking {user} [{i} / {n}]")
            try:
                hit_cache = await gql.find_contributions_for_user(
                    cont, user, args.cache
                )
                if hit_cache:
                    hits.append(user)
                else:
                    misses.append(user)
            except Exception as e:
                sprint(f"Failed to get contributions for {user}\n{e}")
                failures.append(user)
                raise e

        coros = [check(i, user) for i, user in enumerate(committer_candidates)]
        # Anything above 1 and this hits a rate limit after a while
        await ngather(1, *coros)
        print(f"Failed to fetch for : {len(failures)} ({failures})")
        print(f"Hit cache for       : {len(hits)} ({hits})")
        print(f"Missed cache for    : {len(misses)} ({misses})")

        if len(misses) == 0:
            print("Ready to summarize! Run 'python contributions.py --summarize'")
        else:
            print("Keep re-running until misses == 0")


def summarize(args):
    path = Path(args.cache)
    if not path.exists():
        print(f"{path} doesn't exist, run 'python contributions.py' first")
        exit(1)

    with open(args.cache) as f:
        content = json.load(f)

    summaries = []
    date_from = content["$$FROM$$"]
    date_to = content["$$TO$$"]
    repos = content["$$REPOS$$"]
    orgs = content["$$ORGS$$"]
    out = ""

    def ul(items, padding=None):
        nonlocal out
        if padding is not None:
            out += f'<ul style="padding-left: {padding}">'
        else:
            out += "<ul>"
        for item in items:
            out += f"<li>{item}</li>"
        out += "</ul>"

    out += "<h2>Contributions</h2>"

    ul([f"From {date_from}", f"To {date_to}", f"Repos {repos}", f"Orgs {orgs}"])

    for login, data in content.items():
        if login.startswith("$$"):
            continue

        total = 0
        report = ""
        details = ""

        issues_created = data["issues_created"]
        commits = data["commits"]
        prs = data["prs"]
        pr_reviews = data["pr_reviews"]
        issue_and_pr_comments = data["issue_and_pr_comments"]
        discuss_posts = data["posts"]
        discuss_participated_topics = data["participated_topics"]

        total += len(issues_created)
        report += f"{len(issues_created)} issue(s)\n"
        details += textwrap.indent(json.dumps(issues_created, indent=2), prefix="  ")
        details += "\n"

        commit_count = sum(x[1]["totalCount"] for x in commits)
        total += commit_count
        report += (
            f"{commit_count} commit(s) in repos {', '.join([x[0] for x in commits])}\n"
        )

        total += len(prs)
        files = sum(pr["changedFiles"] for pr in prs)
        additions = sum(pr["additions"] for pr in prs)
        deletions = sum(pr["deletions"] for pr in prs)
        report += f"{len(prs)} PR(s) (+{additions}, -{deletions} lines across {files} files)\n"
        details += textwrap.indent(json.dumps(prs, indent=2), prefix="  ")
        details += "\n"

        total += len(pr_reviews)
        report += f"{len(pr_reviews)} PR review(s)\n"
        details += textwrap.indent(json.dumps(pr_reviews, indent=2), prefix="  ")
        details += "\n"

        total += len(issue_and_pr_comments)
        report += f"{len(issue_and_pr_comments)} issue/PR comments\n"
        details += textwrap.indent(
            json.dumps(issue_and_pr_comments, indent=2), prefix="  "
        )
        details += "\n"

        total += len(discuss_posts)
        report += f"{len(discuss_posts)} Discuss posts (across {len(discuss_participated_topics)} topics)"
        details += textwrap.indent(json.dumps(discuss_posts, indent=2), prefix="  ")
        details += "\n"
        details += textwrap.indent(
            json.dumps(discuss_participated_topics, indent=2), prefix="  "
        )
        details += "\n"

        summaries.append(
            {
                "login": login,
                "total": total,
                "data": data,
            }
        )

    summaries = list(reversed(sorted(summaries, key=lambda x: x["total"])))

    out += "<br />"

    for summary in summaries:
        if summary["total"] == 0:
            continue

        data = summary["data"]
        issues_created = data["issues_created"]
        commits = data["commits"]
        prs = data["prs"]
        pr_reviews = data["pr_reviews"]
        issue_and_pr_comments = data["issue_and_pr_comments"]
        discuss_posts = data["posts"]
        discuss_participated_topics = data["participated_topics"]

        out += f'<h3><a href="https://github.com/{summary["login"]}">{summary["login"]}</a> ({summary["total"]} total)</h3>'
        out += "<ul>"
        out += f"<li>{len(commits)} commits</li>"
        out += f"<li>{len(prs)} PR(s) (+{additions}, -{deletions} lines across {files} files)</li>"
        out += '<ul style="padding-left: 20px">'
        for pr in prs:
            out += f'<li><a href="{pr["url"]}">{pr["title"]}</a></li>'
        out += "</ul>"
        out += f"<li>{len(pr_reviews)} PR reviews created</li>"

        out += '<ul style="padding-left: 20px">'
        for review in pr_reviews:
            out += f'<li><a href="{review["url"]}">{review["url"]}</a></li>'
        out += "</ul>"

        out += f"<li>{len(issue_and_pr_comments)} PR/issues comments</li>"
        out += '<ul style="padding-left: 20px">'
        for comment in issue_and_pr_comments:
            out += f'<li><a href="{comment["url"]}">{comment["url"]}</a></li>'
        out += "</ul>"

        out += f"<li>{len(discuss_posts)} Discuss posts (across {len(discuss_participated_topics)} topics)</li>"
        out += '<ul style="padding-left: 20px">'
        for post in discuss_posts:
            out += f'<li><a href="{post["url"]}">{post["url"]}</a></li>'
        out += "</ul>"
        out += "</ul>"

    p = Path("contribution_report.html")
    with open(p, "w") as f:
        f.write(out)

    print(f"Wrote {p.name}, see it at file://{str(p.resolve())}")


if __name__ == "__main__":
    help = "Find TVM contributors"
    parser = argparse.ArgumentParser(description=help)
    parser.add_argument(
        "--date-from",
        help="ISO 8601 date to start looking at contributions (default: 4 weeks ago - now)",
    )
    parser.add_argument(
        "--date-to", help="ISO 8601 date to stop looking at contributions"
    )
    parser.add_argument(
        "--repos", default="apache/tvm,apache/tvm-rfcs", help="GitHub repos to search"
    )
    parser.add_argument("--cache", default="cache.json", help="cache results file")
    parser.add_argument(
        "--summarize", action="store_true", help="read from cache, don't fetch anything"
    )
    parser.add_argument(
        "--organizations",
        default="apache,tlc-pack",
        help="comma separated list of GitHub organizations to filter by",
    )
    args = parser.parse_args()

    if args.summarize:
        summarize(args)
    else:
        asyncio.run(main(args))

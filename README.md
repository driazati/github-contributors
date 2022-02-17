# Usage

```bash
git clone https://github.com/driazati/github-contributors.git
cd github-contributors

pip install -r requirements.txt

python contributions.py --help

# Run this a few times until it says it's done
export GITHUB_TOKEN=ghp_1234567
python contributions.py

python contributions.py --summarize
```

Notes:

- Commit count and links may not match since the link doesn't show commits on which the user was a co-author

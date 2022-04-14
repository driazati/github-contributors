# Usage

```bash
git clone https://github.com/driazati/github-contributors.git
cd github-contributors

pip install -r requirements.txt

python contributions.py --help

# Generate at https://github.com/settings/tokens
export GITHUB_TOKEN=ghp_1234567

# Run this a few times until it says it's done
python contributions.py

# Output information into 'contributions_report.html'
python contributions.py --summarize
```

Notes:

- Commit count and links may not match since the link doesn't show commits on which the user was a co-author

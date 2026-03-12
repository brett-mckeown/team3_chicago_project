# Team 3 Chicago Project

Data analysis and pipeline project for Team 3.

## Prerequisites

- Python 3.10+
- Git
- Homebrew (macOS) or apt (Linux) for installing gitleaks

## Setup Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/brett-mckeown/team3_chicago_project.git
cd team3_chicago_project
```

### 2. Create and Activate Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install Project Dependencies

```bash
pip install -r requirements.txt
```

### 4. Set Up Pre-commit Hooks

Pre-commit hooks run locally before committing to catch issues early (linting, secret detection, code formatting).

```bash
pip install pre-commit
pre-commit install
```

### 5. (Optional) Install Gitleaks Locally

If you want the pre-commit gitleaks hook to work locally:

**macOS:**
```bash
brew install gitleaks
```

**Linux:**
```bash
apt-get install gitleaks
```

**Windows:**
```bash
choco install gitleaks
```

### 6. Verify Setup

Test all pre-commit hooks, linters, and formatters:

```bash
pre-commit run --all-files  # Run all pre-commit hooks
black --check .              # Check code formatting
flake8 .                     # Check code style and errors
```

## Project Structure

```
.
├── README.md                 # This file
├── requirements.txt          # Python dependencies
├── notebooks/               # Jupyter notebooks
│   ├── ecommerce/
│   └── food_inspections/
├── pipelines/               # Data processing pipelines
│   ├── ecommerce/
│   └── food_inspections/
├── resources/               # Data and configuration files
│   └── expectations/        # Great Expectations configurations
├── tests/                   # Test files
└── .github/workflows/       # GitHub Actions CI/CD
    ├── lint.yml            # Linting, formatting, secret detection
    └── snyk.yml            # Security vulnerability scanning
```

## Development Workflow

### Before Committing

Pre-commit hooks will automatically run on `git commit`:
- **gitleaks** — detects exposed secrets
- **detect-secrets** — finds API keys, passwords, etc.
- **black** — auto-formats Python code
- **flake8** — checks style and errors

If any hook fails, fix the issues and try committing again.

### CI/CD Pipeline

On every `push` and `pull_request`, GitHub Actions runs:

1. **Lint workflow** ([.github/workflows/lint.yml](.github/workflows/lint.yml))
   - gitleaks secret scanning
   - detect-secrets
   - black formatting check
   - flake8 style check

2. **Snyk Security Scan** ([.github/workflows/snyk.yml](.github/workflows/snyk.yml))
   - Dependency vulnerability scanning
   - Runs on all branches (test only)
   - Monitors project only on `main` branch pushes

## Dependencies

See [requirements.txt](requirements.txt) for all Python package dependencies.

Key packages:
- **pyspark** — Distributed data processing
- **delta-spark** — Delta Lake format support
- **great-expectations** — Data validation
- **black** — Code formatter
- **flake8** — Code linter
- **pre-commit** — Git hook framework
- **detect-secrets** — Secret detection

## Troubleshooting

### Pre-commit hooks failing locally?

1. Ensure all dependencies are installed:
   ```bash
   pip install -r requirements.txt
   ```

2. Re-install pre-commit hooks:
   ```bash
   pre-commit install
   ```

3. Run all hooks manually to see detailed errors:
   ```bash
   pre-commit run --all-files
   ```

### Gitleaks hook not running?

Ensure gitleaks is installed via your system package manager (see step 5 above).

### GitHub Actions failing?

Check the workflow logs in the **Actions** tab on GitHub. Common issues:
- Invalid or missing SNYK_TOKEN secret (Snyk scan only)
- Python version mismatch (should be 3.10+)
- Missing or outdated dependencies

## Contributing

1. Create a feature branch
2. Make changes and commit (pre-commit hooks will run)
3. Push to GitHub
4. Open a pull request (GitHub Actions will check linting and security)

## License

TBD

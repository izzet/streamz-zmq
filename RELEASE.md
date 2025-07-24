# Release Management Guide

## Overview
This project uses GitHub Releases for version management with automated PyPI publishing.

## Release Process

### 1. Prepare Release
1. Update version in `pyproject.toml`
2. Update `CHANGELOG.md` (if you have one)
3. Commit changes: `git commit -m "Bump version to X.Y.Z"`
4. Push to main: `git push origin main`

### 2. Create GitHub Release
1. Go to: https://github.com/izzet/streamz-zmq/releases
2. Click "Create a new release"
3. **Tag version**: `vX.Y.Z` (e.g., `v0.1.0`)
4. **Release title**: `streamz-zmq vX.Y.Z`
5. **Description**: Add release notes (features, fixes, breaking changes)
6. Click "Publish release"

### 3. Automated Actions
- GitHub Actions will automatically:
  - Run tests and linting
  - Build the package
  - Publish to PyPI

## PyPI Trusted Publishing Setup

**IMPORTANT**: Before your first release, set up PyPI trusted publishing:

1. Go to https://pypi.org/manage/account/publishing/
2. Add a new publisher:
   - **PyPI project name**: `streamz-zmq`
   - **Owner**: `izzet`
   - **Repository name**: `streamz-zmq`
   - **Workflow filename**: `release.yml`
   - **Environment name**: (leave empty)

## Version Numbering
- Follow [Semantic Versioning](https://semver.org/): `MAJOR.MINOR.PATCH`
- Example progression: `0.1.0` → `0.1.1` → `0.2.0` → `1.0.0`

## Pre-release Testing
```bash
# Test locally before release
uv run pytest
uv run ruff check .
uv run ruff format --check .
uv build
```

## Manual PyPI Upload (Emergency)
If GitHub Actions fails:
```bash
uv build
uv publish --token $PYPI_TOKEN
```

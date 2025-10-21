# Build and Release Guide

This document explains how to create releases of the HRS Linkage Tool GUI application.

## Overview

The GitHub Actions workflow automatically builds standalone executables for macOS and Windows whenever you push a version tag.

## Creating a Release

### 1. Update Version

First, update the version in `pyproject.toml`:

```toml
[project]
version = "0.2.0"  # Update this version
```

### 2. Commit Changes

```bash
git add pyproject.toml
git commit -m "Bump version to 0.2.0"
git push origin main
```

### 3. Create and Push Tag

```bash
# Create an annotated tag
git tag -a v0.2.0 -m "Release version 0.2.0

- Feature 1
- Feature 2
- Bug fixes
"

# Push the tag to GitHub
git push origin v0.2.0
```

### 4. Wait for Build

The GitHub Actions workflow will automatically:
1. Build macOS application (HRSLinkageTool.app)
2. Build Windows application (HRSLinkageTool.exe)
3. Create a GitHub Release with both builds attached
4. Generate release notes from your tag message
5. Update README.md with the latest version and download links
6. Commit the updated README back to the main branch

You can monitor the build progress at:
`https://github.com/njw0709/linkdata/actions`

### 5. Download Links Update Automatically

After each release, the workflow automatically:
- Updates the README.md with the latest version number
- Ensures download links point to the correct repository
- Commits the changes back to the main branch

The README will display:
- Latest Version: (e.g., v0.2.0)
- Download links that always point to `/latest/` release:
  - macOS: `https://github.com/njw0709/linkdata/releases/latest/download/HRSLinkageTool-macOS.zip`
  - Windows: `https://github.com/njw0709/linkdata/releases/latest/download/HRSLinkageTool-Windows.zip`

## Testing Locally Before Release

### Test PyInstaller Build on macOS

```bash
# Install PyInstaller
uv pip install pyinstaller

# Build the app
uv run pyinstaller gui_app.spec

# The built app will be in dist/HRSLinkageTool.app
# Test it by running:
open dist/HRSLinkageTool.app
```

### Test PyInstaller Build on Windows

```powershell
# Install PyInstaller
uv pip install pyinstaller

# Build the app
uv run pyinstaller gui_app.spec

# The built app will be in dist\HRSLinkageTool\HRSLinkageTool.exe
# Test it by running:
.\dist\HRSLinkageTool\HRSLinkageTool.exe
```

## Troubleshooting

### Missing Modules

If the built application crashes due to missing modules, add them to the `hiddenimports` list in `gui_app.spec`:

```python
hidden_imports = [
    'pandas',
    'your_missing_module',  # Add here
]
```

### Data Files Not Found

If your application needs data files, add them to the `datas` list in `gui_app.spec`:

```python
datas = [
    ('path/to/data/file', 'destination/folder'),
]
```

### macOS Gatekeeper Issues

macOS will warn about unsigned applications. Users should:
1. Right-click the app
2. Select "Open"
3. Click "Open" in the dialog

### Windows Defender Warnings

Windows Defender may flag PyInstaller builds. This is common and expected. Users should:
1. Click "More info"
2. Click "Run anyway"

## Manual Workflow Trigger

You can also manually trigger the build workflow from GitHub:

1. Go to Actions tab
2. Select "Build and Release" workflow
3. Click "Run workflow"
4. Choose the branch
5. Click "Run workflow"

This is useful for testing the build process without creating a tag.

## Version Numbering

Follow semantic versioning (semver):
- Major version: Breaking changes (1.0.0 → 2.0.0)
- Minor version: New features, backward compatible (1.0.0 → 1.1.0)
- Patch version: Bug fixes (1.0.0 → 1.0.1)

Tag format: `vMAJOR.MINOR.PATCH` (e.g., v1.2.3)

## First Release Checklist

Before creating your first release (v0.1.0):

- [ ] Test PyInstaller build locally on macOS (if available)
- [ ] Test PyInstaller build locally on Windows (if available)
- [ ] Verify all GUI pages load correctly
- [ ] Test file selection dialogs
- [ ] Test data loading functionality
- [ ] Test the actual linkage process
- [ ] Update README with your actual GitHub username/repo name
- [ ] Create and push the v0.1.0 tag

## Automatic README Updates

The workflow automatically updates the README after each release. The `scripts/update_readme.py` script:

1. Extracts release information from the GitHub API
2. Updates download links to ensure they point to `njw0709/linkdata`
3. Adds/updates the **Latest Version** badge
4. Commits changes back to the main branch

This ensures the README always reflects:
- The current repository location
- The latest version number
- Working download links

**Note:** If you fork this repository, the script will automatically update the links to point to your fork after the first release.


# Build and Release Guide

This document explains how to create releases of the HRS Linkage Tool GUI application.

## Overview

The GitHub Actions workflow automatically builds Windows executables whenever you push a version tag. macOS builds must be compiled locally and uploaded manually due to Qt compatibility issues on GitHub Actions runners.

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

### 4. Wait for Automated Windows Build

The GitHub Actions workflow will automatically:
1. Build Windows application (HRSLinkageTool.exe)
2. Create a **DRAFT** GitHub Release with the Windows build attached
3. Generate release notes from your tag message

You can monitor the build progress at:
`https://github.com/njw0709/linkdata/actions`

### 5. Build and Upload macOS Builds

Since macOS builds must be done locally, follow these steps:

#### Build macOS Application Locally

```bash
# Ensure you have the latest code
git checkout v0.2.0  # Use your version tag

# Clean previous builds
rm -rf build/ dist/

# Install dependencies
uv sync
uv pip install pyinstaller==6.3.0

# Build the application
uv run pyinstaller gui_app.spec

# Test the built application
open dist/HRSLinkageTool.app

# Create ZIP archive for upload
cd dist
zip -r HRSLinkageTool-macOS-ARM.zip HRSLinkageTool.app
cd ..
```

#### Upload macOS Build to Release

1. Go to the [Releases page](https://github.com/njw0709/linkdata/releases)
2. Find your DRAFT release (created by GitHub Actions)
3. Click "Edit release"
4. Drag and drop `dist/HRSLinkageTool-macOS-ARM.zip` to the assets section
5. Update the release notes if needed
6. Click "Publish release" (this changes it from draft to published)

**Note:** Download links in the README use `/latest/` which automatically points to the newest published release, so no manual updates are needed.

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

## Release Checklist

For each release:

- [ ] Update version in `pyproject.toml`
- [ ] Commit and push changes
- [ ] Create and push version tag (e.g., v0.2.0)
- [ ] Wait for GitHub Actions to build Windows version and create DRAFT release
- [ ] Build macOS application locally
- [ ] Test macOS application works correctly
- [ ] Create ZIP archive of macOS build
- [ ] Upload macOS ZIP to the draft release on GitHub
- [ ] Update release notes if needed
- [ ] Publish the release (changes from draft to published)

## MacOS manual build

```bash
# Ensure you have the latest code
git checkout v0.2.0  # Use your version tag

# Clean previous builds
rm -rf build/ dist/

# Install dependencies
uv sync
uv pip install pyinstaller==6.3.0

# Build the application
uv run pyinstaller gui_app.spec

# Test the built application
open dist/HRSLinkageTool.app

# Create ZIP archive for upload
cd dist
zip -r HRSLinkageTool-macOS-ARM.zip HRSLinkageTool.app
cd ..
```
Then, upload the ZIP file to the release on GitHub.



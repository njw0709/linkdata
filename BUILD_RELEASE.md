# Build and Release Guide

This document explains how to create releases of the STITCH GUI application.

## Overview

The GitHub Actions workflow automatically builds standalone executables for macOS (ARM) and Windows whenever you push a version tag.

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
1. Build macOS ARM application (STITCH.app) using `ditto` for proper ZIP packaging
2. Build Windows application (STITCH.exe)
3. Create a GitHub Release with both builds attached
4. Generate release notes from your tag message

You can monitor the build progress at:
`https://github.com/njw0709/stitch/actions`

**Note:** Download links in the README use `/latest/` which automatically points to the newest published release.

## Testing Locally Before Release

### Test PyInstaller Build on macOS

```bash
# Clean previous builds
rm -rf build/ dist/

# Install PyInstaller
uv pip install pyinstaller==6.3.0

# Build the app
uv run pyinstaller gui_app.spec

# The built app will be in dist/STITCH.app
# Test it by running:
open dist/STITCH.app

# Create ZIP archive using ditto (preserves macOS attributes properly)
cd dist
ditto -c -k --sequesterRsrc --keepParent STITCH.app STITCH-macOS-ARM.zip
cd ..
```

### Test PyInstaller Build on Windows

```powershell
# Install PyInstaller
uv pip install pyinstaller

# Build the app
uv run pyinstaller gui_app.spec

# The built app will be in dist\STITCH\STITCH.exe
# Test it by running:
.\dist\STITCH\STITCH.exe
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
- [ ] Wait for GitHub Actions to build macOS and Windows versions
- [ ] Monitor build progress at Actions tab
- [ ] Verify release is created with both platform builds
- [ ] Test downloaded builds on actual machines (optional)



# GitHub Actions Workflows

This directory contains automated workflows for the HRS Linkage Tool.

## build-release.yml

Automated multi-platform build and release workflow.

### Trigger

- **Automatic**: Pushes to tags matching `v*.*.*` (e.g., `v0.1.0`, `v1.2.3`)
- **Manual**: Can be triggered via GitHub Actions UI

### Workflow Steps

1. **build-macos**: Build macOS application
   - Runs on `macos-latest`
   - Uses PyInstaller to create `.app` bundle
   - Creates ZIP archive: `HRSLinkageTool-macOS.zip`

2. **build-windows**: Build Windows application
   - Runs on `windows-latest`
   - Uses PyInstaller to create `.exe` application
   - Creates ZIP archive: `HRSLinkageTool-Windows.zip`

3. **create-release**: Create GitHub Release
   - Depends on successful macOS and Windows builds
   - Creates GitHub Release with version tag
   - Uploads both platform builds as release assets
   - Generates release notes from commits

4. **update-readme**: Update README automatically
   - Depends on successful release creation
   - Runs Python script to update `README.md`
   - Updates download links to point to correct repository
   - Adds/updates **Latest Version** badge
   - Commits and pushes changes back to `main` branch

### Dependencies

- Python 3.11
- [astral-sh/setup-uv](https://github.com/astral-sh/setup-uv) - For dependency management
- [softprops/action-gh-release](https://github.com/softprops/action-gh-release) - For creating releases

### Environment Variables

- `GITHUB_TOKEN`: Automatically provided by GitHub Actions for API access

### Permissions Required

```yaml
permissions:
  contents: write  # Required for creating releases and pushing to repository
```

### Outputs

For each release, the workflow produces:
- **GitHub Release** with version tag and release notes
- **macOS Build**: `HRSLinkageTool-macOS.zip` containing `.app` bundle
- **Windows Build**: `HRSLinkageTool-Windows.zip` containing `.exe` and dependencies
- **Updated README.md**: With current version and correct download links

### Example Usage

```bash
# Create a new release
git tag -a v0.2.0 -m "Release v0.2.0

New features:
- Feature 1
- Feature 2

Bug fixes:
- Fix 1
"
git push origin v0.2.0
```

### Monitoring

View workflow runs at:
`https://github.com/njw0709/linkdata/actions/workflows/build-release.yml`

### Troubleshooting

**Build fails on macOS or Windows:**
- Check PyInstaller spec file (`gui_app.spec`)
- Ensure all dependencies are listed in `pyproject.toml`
- Check for hidden import issues

**README not updating:**
- Verify the workflow has `contents: write` permission
- Check the `scripts/update_readme.py` script for errors
- Ensure the `main` branch exists and is the default branch

**Release not created:**
- Verify tag format matches `v*.*.*`
- Check that both build jobs completed successfully
- Verify `GITHUB_TOKEN` has correct permissions


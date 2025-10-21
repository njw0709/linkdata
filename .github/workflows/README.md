# GitHub Actions Workflows

This directory contains automated workflows for the HRS Linkage Tool.

## build-release.yml

Automated multi-platform build and release workflow.

### Trigger

- **Automatic**: Pushes to tags matching `v*.*.*` (e.g., `v0.1.0`, `v1.2.3`)
- **Manual**: Can be triggered via GitHub Actions UI

### Workflow Steps

1. **build-windows**: Build Windows application
   - Runs on `windows-latest`
   - Uses PyInstaller to create `.exe` application
   - Creates ZIP archive: `HRSLinkageTool-Windows.zip`

2. **create-release**: Create DRAFT GitHub Release
   - Depends on successful Windows build
   - Creates DRAFT GitHub Release with version tag
   - Uploads Windows build as release asset
   - Generates release notes from commits
   - Waits for manual upload of macOS builds before publishing

**Note:** macOS builds are compiled locally and uploaded manually due to Qt compatibility issues on GitHub Actions runners.

### Dependencies

- Python 3.9
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
- **DRAFT GitHub Release** with version tag and release notes
- **Windows Build**: `HRSLinkageTool-Windows.zip` containing `.exe` and dependencies

After manually uploading macOS builds and publishing the release:
- **Published GitHub Release** with both Windows and macOS builds

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

**Windows build fails:**
- Check PyInstaller spec file (`gui_app.spec`)
- Ensure all dependencies are listed in `pyproject.toml`
- Check for hidden import issues

**macOS local build fails:**
- Ensure you're using Python 3.9
- Try: `rm -rf .venv && uv sync`
- Check Qt plugins are correctly excluded in `gui_app.spec`
- See `BUILD_RELEASE.md` for detailed build instructions

**Release not created:**
- Verify tag format matches `v*.*.*`
- Check that Windows build job completed successfully
- Verify `GITHUB_TOKEN` has correct permissions


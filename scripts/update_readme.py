#!/usr/bin/env python3
"""
Update README.md with the latest release information.

This script updates the download links and version information in the README
after a new release is created.
"""

import json
import re
import sys
from pathlib import Path


def update_readme_with_release(release_data_json: str, readme_path: Path):
    """
    Update README.md with release information.

    Args:
        release_data_json: JSON string containing release data from GitHub API
        readme_path: Path to README.md file
    """
    # Parse release data
    release_data = json.loads(release_data_json)
    tag_name = release_data["tag_name"]
    version = tag_name.lstrip("v")  # Remove 'v' prefix if present
    repo_name = release_data["html_url"].split("/")[3:5]  # Extract owner/repo
    repo_full = f"{repo_name[0]}/{repo_name[1]}"

    # Read current README
    readme_content = readme_path.read_text()

    # Update download links to ensure they point to the correct repository
    # Pattern: https://github.com/{any-owner}/{any-repo}/releases/latest/download/HRSLinkageTool-{platform}.zip
    macos_pattern = r"https://github\.com/[^/]+/[^/]+/releases/latest/download/HRSLinkageTool-macOS\.zip"
    windows_pattern = r"https://github\.com/[^/]+/[^/]+/releases/latest/download/HRSLinkageTool-Windows\.zip"

    new_macos_url = f"https://github.com/{repo_full}/releases/latest/download/HRSLinkageTool-macOS.zip"
    new_windows_url = f"https://github.com/{repo_full}/releases/latest/download/HRSLinkageTool-Windows.zip"

    # Replace download URLs
    readme_content = re.sub(macos_pattern, new_macos_url, readme_content)
    readme_content = re.sub(windows_pattern, new_windows_url, readme_content)

    # Add or update version badge/information if there's a Quick Start section
    version_badge = f"**Latest Version:** {tag_name}"

    # Check if we already have a version line and update it, or add it after "Quick Start" header
    if "**Latest Version:**" in readme_content:
        # Update existing version
        readme_content = re.sub(
            r"\*\*Latest Version:\*\* v[\d.]+", version_badge, readme_content
        )
    else:
        # Add version info after "Quick Start (Standalone Application)" header
        quick_start_pattern = r"(### Quick Start \(Standalone Application\)\n)"
        replacement = f"\\1\n{version_badge}\n"
        readme_content = re.sub(quick_start_pattern, replacement, readme_content)

    # Write updated README
    readme_path.write_text(readme_content)

    print(f"✓ Updated README.md with release {tag_name}")
    print(f"  Repository: {repo_full}")
    print(f"  macOS URL: {new_macos_url}")
    print(f"  Windows URL: {new_windows_url}")


def main():
    if len(sys.argv) != 2:
        print("Usage: update_readme.py <release_data_json>", file=sys.stderr)
        sys.exit(1)

    release_data_json = sys.argv[1]

    # Find README.md (script is in scripts/, README is in parent directory)
    script_dir = Path(__file__).parent
    readme_path = script_dir.parent / "README.md"

    if not readme_path.exists():
        print(f"Error: README.md not found at {readme_path}", file=sys.stderr)
        sys.exit(1)

    update_readme_with_release(release_data_json, readme_path)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Test script for update_readme.py

This script tests the README update functionality with sample release data.
"""

import json
import tempfile
from pathlib import Path
from update_readme import update_readme_with_release


def test_update_readme():
    """Test README update with sample release data."""

    # Sample release data (mimics GitHub API response)
    sample_release = {
        "tag_name": "v0.1.0",
        "name": "Release v0.1.0",
        "html_url": "https://github.com/njw0709/linkdata/releases/tag/v0.1.0",
        "assets": [
            {
                "name": "HRSLinkageTool-macOS.zip",
                "browser_download_url": "https://github.com/njw0709/linkdata/releases/download/v0.1.0/HRSLinkageTool-macOS.zip",
            },
            {
                "name": "HRSLinkageTool-Windows.zip",
                "browser_download_url": "https://github.com/njw0709/linkdata/releases/download/v0.1.0/HRSLinkageTool-Windows.zip",
            },
        ],
    }

    # Sample README content
    sample_readme = """# Test README

## Installation

### Quick Start (Standalone Application)

Download the pre-built standalone application for your platform:

- **macOS**: [Download HRSLinkageTool-macOS.zip](https://github.com/oldowner/oldrepo/releases/latest/download/HRSLinkageTool-macOS.zip)
  - Extract the ZIP file and run `HRSLinkageTool.app`

- **Windows**: [Download HRSLinkageTool-Windows.zip](https://github.com/oldowner/oldrepo/releases/latest/download/HRSLinkageTool-Windows.zip)
  - Extract the ZIP file and run `HRSLinkageTool.exe`

### Build from Source

Instructions here...
"""

    # Create temporary README file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".md", delete=False) as tmp:
        tmp.write(sample_readme)
        tmp_path = Path(tmp.name)

    try:
        # Test the update function
        release_json = json.dumps(sample_release)
        update_readme_with_release(release_json, tmp_path)

        # Read updated content
        updated_content = tmp_path.read_text()

        # Verify updates
        print("✓ Testing README update functionality\n")
        print("Updated README content:")
        print("=" * 60)
        print(updated_content)
        print("=" * 60)

        # Check assertions
        assert "njw0709/linkdata" in updated_content, "Repository not updated"
        assert (
            "**Latest Version:** v0.1.0" in updated_content
        ), "Version badge not added"
        assert "oldowner/oldrepo" not in updated_content, "Old repository still present"

        print("\n✓ All tests passed!")
        print("  - Repository URLs updated correctly")
        print("  - Version badge added")
        print("  - Old URLs replaced")

    finally:
        # Clean up
        tmp_path.unlink()


if __name__ == "__main__":
    test_update_readme()

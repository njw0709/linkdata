#!/usr/bin/env python
"""
GUI application entry point for the HRS Linkage Tool.

This script launches a PyQt6-based wizard interface that guides users through
configuring and executing the lagged contextual data linkage pipeline.

Usage:
    python gui_app.py
    # or
    uv run python gui_app.py
"""
import sys

from PyQt6.QtWidgets import QApplication

from linkdata.gui.main_window import LinkageWizard


def main():
    """Launch the HRS Linkage Tool GUI."""
    app = QApplication(sys.argv)

    # Set application metadata
    app.setApplicationName("HRS Linkage Tool")
    app.setOrganizationName("HRS Research")

    # Create and show wizard
    wizard = LinkageWizard()
    wizard.show()

    # Run application
    sys.exit(app.exec())


if __name__ == "__main__":
    main()

"""
File and directory picker widgets.
"""

from pathlib import Path
from typing import Optional, Callable

from PyQt6.QtWidgets import QWidget, QHBoxLayout, QLineEdit, QPushButton, QFileDialog
from PyQt6.QtCore import pyqtSignal


class FilePicker(QWidget):
    """
    Widget for selecting a file with a browse button.
    """

    fileSelected = pyqtSignal(str)  # Emitted when a file is selected

    def __init__(
        self,
        parent=None,
        file_filter: str = "All Files (*)",
        validator: Optional[Callable[[str], bool]] = None,
    ):
        super().__init__(parent)
        self.file_filter = file_filter
        self.validator = validator

        # Layout
        layout = QHBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)

        # Path input
        self.path_edit = QLineEdit()
        self.path_edit.setPlaceholderText("No file selected")
        self.path_edit.textChanged.connect(self._on_path_changed)

        # Browse button
        self.browse_btn = QPushButton("Browse...")
        self.browse_btn.clicked.connect(self._browse)

        layout.addWidget(self.path_edit, 1)
        layout.addWidget(self.browse_btn)

        self.setLayout(layout)

    def _browse(self):
        """Open file dialog."""
        file_path, _ = QFileDialog.getOpenFileName(
            self, "Select File", "", self.file_filter
        )

        if file_path:
            self.set_path(file_path)

    def _on_path_changed(self, path: str):
        """Handle path text changes."""
        if path and Path(path).exists():
            self.fileSelected.emit(path)

    def set_path(self, path: str):
        """Set the file path."""
        self.path_edit.setText(path)
        if path:
            self.fileSelected.emit(path)

    def get_path(self) -> str:
        """Get the current file path."""
        return self.path_edit.text()

    def is_valid(self) -> bool:
        """Check if current path is valid."""
        path = self.get_path()
        if not path:
            return False
        if not Path(path).exists():
            return False
        if self.validator and not self.validator(path):
            return False
        return True


class DirectoryPicker(QWidget):
    """
    Widget for selecting a directory with a browse button.
    """

    directorySelected = pyqtSignal(str)  # Emitted when a directory is selected

    def __init__(self, parent=None):
        super().__init__(parent)

        # Layout
        layout = QHBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)

        # Path input
        self.path_edit = QLineEdit()
        self.path_edit.setPlaceholderText("No directory selected")
        self.path_edit.textChanged.connect(self._on_path_changed)

        # Browse button
        self.browse_btn = QPushButton("Browse...")
        self.browse_btn.clicked.connect(self._browse)

        layout.addWidget(self.path_edit, 1)
        layout.addWidget(self.browse_btn)

        self.setLayout(layout)

    def _browse(self):
        """Open directory dialog."""
        dir_path = QFileDialog.getExistingDirectory(self, "Select Directory", "")

        if dir_path:
            self.set_path(dir_path)

    def _on_path_changed(self, path: str):
        """Handle path text changes."""
        if path and Path(path).exists():
            self.directorySelected.emit(path)

    def set_path(self, path: str):
        """Set the directory path."""
        self.path_edit.setText(path)
        if path:
            self.directorySelected.emit(path)

    def get_path(self) -> str:
        """Get the current directory path."""
        return self.path_edit.text()

    def is_valid(self) -> bool:
        """Check if current path is valid."""
        path = self.get_path()
        if not path:
            return False
        return Path(path).exists() and Path(path).is_dir()

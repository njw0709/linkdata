"""
Tests for GUI widgets.

Note: These tests use Qt's offscreen platform in headless environments (configured in conftest.py).
"""

import pytest
from pathlib import Path

# Check if PyQt6 is available
pytest.importorskip("PyQt6")

from PyQt6.QtWidgets import QApplication
from PyQt6.QtTest import QTest
from PyQt6.QtCore import Qt
import pandas as pd

from stitch.gui.widgets.file_picker import FilePicker, DirectoryPicker
from stitch.gui.widgets.data_preview_table import DataPreviewTable


class TestFilePicker:
    """Test FilePicker widget."""

    def test_file_picker_creation(self, qapp):
        """Test that FilePicker can be created."""
        picker = FilePicker()
        assert picker is not None
        assert picker.get_path() == ""

    def test_file_picker_set_path(self, qapp, tmp_path):
        """Test setting a file path."""
        test_file = tmp_path / "test.txt"
        test_file.write_text("test")

        picker = FilePicker()
        picker.set_path(str(test_file))
        assert picker.get_path() == str(test_file)

    def test_file_picker_is_valid(self, qapp, tmp_path):
        """Test path validation."""
        test_file = tmp_path / "test.txt"
        test_file.write_text("test")

        picker = FilePicker()
        assert picker.is_valid() is False  # No path set

        picker.set_path(str(test_file))
        assert picker.is_valid() is True  # Valid file

        picker.set_path("/nonexistent/file.txt")
        assert picker.is_valid() is False  # Invalid path

    def test_file_picker_signal(self, qapp, tmp_path, qtbot):
        """Test fileSelected signal emission."""
        test_file = tmp_path / "test.txt"
        test_file.write_text("test")

        picker = FilePicker()

        with qtbot.waitSignal(picker.fileSelected, timeout=1000):
            picker.set_path(str(test_file))


class TestDirectoryPicker:
    """Test DirectoryPicker widget."""

    def test_directory_picker_creation(self, qapp):
        """Test that DirectoryPicker can be created."""
        picker = DirectoryPicker()
        assert picker is not None
        assert picker.get_path() == ""

    def test_directory_picker_set_path(self, qapp, tmp_path):
        """Test setting a directory path."""
        picker = DirectoryPicker()
        picker.set_path(str(tmp_path))
        assert picker.get_path() == str(tmp_path)

    def test_directory_picker_is_valid(self, qapp, tmp_path):
        """Test path validation."""
        picker = DirectoryPicker()
        assert picker.is_valid() is False  # No path set

        picker.set_path(str(tmp_path))
        assert picker.is_valid() is True  # Valid directory

        picker.set_path("/nonexistent/directory")
        assert picker.is_valid() is False  # Invalid path

    def test_directory_picker_signal(self, qapp, tmp_path, qtbot):
        """Test directorySelected signal emission."""
        picker = DirectoryPicker()

        with qtbot.waitSignal(picker.directorySelected, timeout=1000):
            picker.set_path(str(tmp_path))


class TestDataPreviewTable:
    """Test DataPreviewTable widget."""

    def test_data_preview_table_creation(self, qapp):
        """Test that DataPreviewTable can be created."""
        table = DataPreviewTable()
        assert table is not None
        assert table.rowCount() == 0
        assert table.columnCount() == 0

    def test_data_preview_table_set_dataframe(self, qapp):
        """Test setting a DataFrame."""
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})

        table = DataPreviewTable()
        table.set_dataframe(df)

        assert table.rowCount() == 3
        assert table.columnCount() == 2
        assert table.get_columns() == ["col1", "col2"]

    def test_data_preview_table_clear(self, qapp):
        """Test clearing the table."""
        df = pd.DataFrame({"col1": [1, 2, 3]})

        table = DataPreviewTable()
        table.set_dataframe(df)
        assert table.rowCount() == 3

        table.set_dataframe(None)
        assert table.rowCount() == 0
        assert table.columnCount() == 0

    def test_data_preview_table_with_nan(self, qapp):
        """Test displaying DataFrame with NaN values."""
        df = pd.DataFrame({"col1": [1, None, 3], "col2": ["a", "b", None]})

        table = DataPreviewTable()
        table.set_dataframe(df)

        assert table.rowCount() == 3
        # NaN values should be displayed as empty strings
        assert table.item(1, 0).text() == ""
        assert table.item(2, 1).text() == ""

    def test_data_preview_table_get_columns(self, qapp):
        """Test getting column names."""
        df = pd.DataFrame({"col1": [1], "col2": [2], "col3": [3]})

        table = DataPreviewTable()
        table.set_dataframe(df)

        columns = table.get_columns()
        assert columns == ["col1", "col2", "col3"]

    def test_data_preview_table_empty_dataframe(self, qapp):
        """Test with empty DataFrame."""
        df = pd.DataFrame()

        table = DataPreviewTable()
        table.set_dataframe(df)

        assert table.rowCount() == 0
        assert table.columnCount() == 0

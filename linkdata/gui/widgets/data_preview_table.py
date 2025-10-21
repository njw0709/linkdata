"""
Data preview table widget for displaying pandas DataFrames.
"""

from typing import Optional

import pandas as pd
from PyQt6.QtWidgets import QTableWidget, QTableWidgetItem, QHeaderView
from PyQt6.QtCore import Qt


class DataPreviewTable(QTableWidget):
    """
    A table widget for previewing pandas DataFrame data.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.setAlternatingRowColors(True)

    def set_dataframe(self, df: Optional[pd.DataFrame]):
        """
        Display a pandas DataFrame in the table.

        Args:
            df: DataFrame to display, or None to clear
        """
        if df is None or df.empty:
            self.clear()
            self.setRowCount(0)
            self.setColumnCount(0)
            return

        # Set dimensions
        self.setRowCount(len(df))
        self.setColumnCount(len(df.columns))

        # Set headers
        self.setHorizontalHeaderLabels(df.columns.tolist())

        # Populate data
        for i in range(len(df)):
            for j, col in enumerate(df.columns):
                value = df.iloc[i, j]
                # Convert to string, handling NaN/None
                if pd.isna(value):
                    text = ""
                else:
                    text = str(value)

                item = QTableWidgetItem(text)
                item.setTextAlignment(
                    Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter
                )
                self.setItem(i, j, item)

        # Resize columns to content
        self.resizeColumnsToContents()

        # Set reasonable maximum widths
        header = self.horizontalHeader()
        if header:
            for i in range(self.columnCount()):
                if self.columnWidth(i) > 300:
                    self.setColumnWidth(i, 300)
            header.setStretchLastSection(True)

    def get_columns(self):
        """
        Get list of column names currently displayed.

        Returns:
            List of column names
        """
        if self.columnCount() == 0:
            return []

        return [self.horizontalHeaderItem(i).text() for i in range(self.columnCount())]

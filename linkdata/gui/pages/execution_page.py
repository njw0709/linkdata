"""
Pipeline execution page.
"""

import sys
import subprocess
from pathlib import Path

from PyQt6.QtWidgets import (
    QWizardPage,
    QVBoxLayout,
    QPushButton,
    QTextEdit,
    QProgressBar,
    QLabel,
    QHBoxLayout,
    QMessageBox,
)
from PyQt6.QtCore import QThread, pyqtSignal, QUrl
from PyQt6.QtGui import QDesktopServices


class PipelineRunner(QThread):
    """Thread for running the pipeline CLI command."""

    output = pyqtSignal(str)  # Emits output lines
    finished_signal = pyqtSignal(bool, str)  # success, message

    def __init__(self, command: list):
        super().__init__()
        self.command = command
        self.process = None

    def run(self):
        """Run the pipeline command."""
        try:
            self.process = subprocess.Popen(
                self.command,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                universal_newlines=True,
            )

            # Stream output
            for line in iter(self.process.stdout.readline, ""):
                if line:
                    self.output.emit(line.rstrip())

            # Wait for completion
            return_code = self.process.wait()

            if return_code == 0:
                self.finished_signal.emit(True, "Pipeline completed successfully!")
            else:
                self.finished_signal.emit(
                    False, f"Pipeline failed with exit code {return_code}"
                )

        except Exception as e:
            self.finished_signal.emit(False, f"Error running pipeline: {str(e)}")

    def stop(self):
        """Stop the running process."""
        if self.process:
            self.process.terminate()
            self.process.wait()


class ExecutionPage(QWizardPage):
    """
    Wizard page for executing the pipeline.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setTitle("Run Pipeline")
        self.setSubTitle("Execute the linkage pipeline with your configured settings.")

        self.runner_thread = None
        self.pipeline_running = False
        self.pipeline_completed = False

        # Create layout
        layout = QVBoxLayout()

        # Instructions
        instructions = QLabel(
            "Click 'Run Pipeline' to start the linkage process. "
            "This may take a while depending on the number of lags and data size."
        )
        instructions.setWordWrap(True)
        layout.addWidget(instructions)

        # Control buttons
        button_layout = QHBoxLayout()

        self.run_button = QPushButton("Run Pipeline")
        self.run_button.clicked.connect(self._run_pipeline)
        button_layout.addWidget(self.run_button)

        self.save_log_button = QPushButton("Save Log")
        self.save_log_button.clicked.connect(self._save_log)
        self.save_log_button.setEnabled(False)
        button_layout.addWidget(self.save_log_button)

        self.open_output_button = QPushButton("Open Output Directory")
        self.open_output_button.clicked.connect(self._open_output_directory)
        self.open_output_button.setEnabled(False)
        button_layout.addWidget(self.open_output_button)

        button_layout.addStretch()
        layout.addLayout(button_layout)

        # Progress bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 0)  # Indeterminate
        self.progress_bar.setVisible(False)
        layout.addWidget(self.progress_bar)

        # Status label
        self.status_label = QLabel("Ready to run.")
        layout.addWidget(self.status_label)

        # Output log
        output_label = QLabel("Pipeline Output:")
        layout.addWidget(output_label)

        self.output_text = QTextEdit()
        self.output_text.setReadOnly(True)
        self.output_text.setFontFamily("Monospace")
        layout.addWidget(self.output_text)

        self.setLayout(layout)

    def _build_command(self) -> list:
        """Build the CLI command from wizard fields."""
        wizard = self.wizard()
        if not wizard:
            return []

        # Get the path to link_lags.py (in the same directory as this package)
        gui_dir = Path(__file__).parent.parent
        linkdata_dir = gui_dir.parent
        project_dir = linkdata_dir.parent
        link_lags_script = project_dir / "link_lags.py"

        command = [
            sys.executable,
            str(link_lags_script),
            "--hrs-data",
            wizard.field("hrs_data_path"),
            "--context-dir",
            wizard.field("context_dir"),
            "--output_name",
            wizard.field("output_name"),
            "--id-col",
            wizard.field("id_col"),
            "--date-col",
            wizard.field("date_col"),
            "--measure-type",
            wizard.field("measure_type"),
            "--save-dir",
            wizard.field("save_dir"),
            "--data-col",
            wizard.field("data_col"),
            "--geoid-col",
            wizard.field("geoid_col"),
            "--n-lags",
            str(wizard.field("n_lags")),
            "--geoid-prefix",
            wizard.field("geoid_prefix"),
        ]

        # Optional: file extension
        file_ext = wizard.field("file_extension")
        if file_ext != "Auto-detect":
            command.extend(["--file-extension", file_ext])

        # Optional: residential history
        if wizard.field("use_residential_hist"):
            command.extend(
                [
                    "--residential-hist",
                    wizard.field("residential_hist_path"),
                    "--res-hist-hhidpn",
                    wizard.field("res_hist_hhidpn"),
                    "--res-hist-movecol",
                    wizard.field("res_hist_movecol"),
                    "--res-hist-mvyear",
                    wizard.field("res_hist_mvyear"),
                    "--res-hist-mvmonth",
                    wizard.field("res_hist_mvmonth"),
                    "--res-hist-moved-mark",
                    wizard.field("res_hist_moved_mark"),
                    "--res-hist-geoid",
                    wizard.field("res_hist_geoid"),
                    "--res-hist-survey-yr-col",
                    wizard.field("res_hist_survey_yr_col"),
                    "--res-hist-first-tract-mark",
                    str(wizard.field("res_hist_first_tract_mark")),
                ]
            )

        # Flags
        if wizard.field("parallel"):
            command.append("--parallel")

        if wizard.field("include_lag_date"):
            command.append("--include-lag-date")

        return command

    def _run_pipeline(self):
        """Start the pipeline execution."""
        if self.pipeline_running:
            QMessageBox.warning(
                self,
                "Pipeline Running",
                "Pipeline is already running. Please wait for it to complete.",
            )
            return

        # Build command
        command = self._build_command()

        # Show command in output
        self.output_text.clear()
        self.output_text.append("=== Command ===")
        self.output_text.append(" ".join(command))
        self.output_text.append("\n=== Output ===")

        # Update UI
        self.pipeline_running = True
        self.pipeline_completed = False
        self.run_button.setEnabled(False)
        self.progress_bar.setVisible(True)
        self.status_label.setText("Running pipeline...")

        # Start runner thread
        self.runner_thread = PipelineRunner(command)
        self.runner_thread.output.connect(self._on_output)
        self.runner_thread.finished_signal.connect(self._on_finished)
        self.runner_thread.start()

    def _on_output(self, line: str):
        """Handle output from pipeline."""
        self.output_text.append(line)

        # Auto-scroll to bottom
        scrollbar = self.output_text.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())

    def _on_finished(self, success: bool, message: str):
        """Handle pipeline completion."""
        self.pipeline_running = False
        self.pipeline_completed = success

        self.run_button.setEnabled(True)
        self.progress_bar.setVisible(False)
        self.save_log_button.setEnabled(True)

        if success:
            self.status_label.setText(f"✓ {message}")
            self.open_output_button.setEnabled(True)
            QMessageBox.information(self, "Success", message)
        else:
            self.status_label.setText(f"✗ {message}")
            QMessageBox.critical(self, "Error", message)

        self.completeChanged.emit()

    def _save_log(self):
        """Save the output log to a file."""
        from PyQt6.QtWidgets import QFileDialog

        file_path, _ = QFileDialog.getSaveFileName(
            self,
            "Save Log File",
            "pipeline_log.txt",
            "Text Files (*.txt);;All Files (*)",
        )

        if file_path:
            try:
                with open(file_path, "w") as f:
                    f.write(self.output_text.toPlainText())
                QMessageBox.information(self, "Saved", f"Log saved to {file_path}")
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Failed to save log: {str(e)}")

    def _open_output_directory(self):
        """Open the output directory in file explorer."""
        wizard = self.wizard()
        if not wizard:
            return

        save_dir = wizard.field("save_dir")
        if save_dir and Path(save_dir).exists():
            QDesktopServices.openUrl(QUrl.fromLocalFile(save_dir))

    def isComplete(self):
        """Page is complete when pipeline has finished successfully."""
        return self.pipeline_completed

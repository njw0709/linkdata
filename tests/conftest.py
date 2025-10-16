"""
Shared pytest fixtures for HRS testing.

This module provides common fixtures that can be used across multiple test modules.
"""

import pytest
from pathlib import Path
from linkdata.hrs import ResidentialHistoryHRS, HRSInterviewData


@pytest.fixture(scope="session")
def test_data_dir():
    """Get the test data directory path."""
    return Path(__file__).parent / "test_data"


@pytest.fixture(scope="session")
def persistent_residential_history(test_data_dir):
    """Create persistent residential history data for session-scoped tests."""
    from .test_hrs_data import create_residential_history_dataframe

    df = create_residential_history_dataframe()
    file_path = test_data_dir / "persistent_residential_history.dta"
    df.to_stata(file_path, write_index=False)

    yield ResidentialHistoryHRS(file_path, first_tract_mark="999.0")

    # Cleanup
    if file_path.exists():
        file_path.unlink()


@pytest.fixture(scope="session")
def persistent_survey_data(test_data_dir):
    """Create persistent survey data for session-scoped tests."""
    from .test_hrs_interview_data import create_survey_dataframe

    df = create_survey_dataframe()
    file_path = test_data_dir / "persistent_survey_data.dta"
    df.to_stata(file_path, write_index=False)

    yield HRSInterviewData(file_path)

    # Cleanup
    if file_path.exists():
        file_path.unlink()


@pytest.fixture(scope="session")
def persistent_combined_data(persistent_residential_history, persistent_survey_data):
    """Create persistent combined data with linked residential history."""
    persistent_survey_data.residential_hist = persistent_residential_history
    return persistent_survey_data


# Import fixtures from other test modules to make them available globally
from .test_hrs_data import (
    residential_history_hrs,
    residential_history_hrs_persistent,
    fake_residential_history_file,
    fake_residential_history_file_persistent,
)
from .test_hrs_interview_data import (
    survey_data_hrs,
    survey_data_hrs_persistent,
    survey_with_residential_history,
    survey_with_residential_history_persistent,
    fake_survey_file,
    fake_survey_file_persistent,
)

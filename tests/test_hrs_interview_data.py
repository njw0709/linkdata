"""
Test fixtures for HRSInterviewData class.

This module provides pytest fixtures that generate fake survey/interview data
in Stata format for testing the HRSInterviewData class functionality.
"""

import pytest
from pathlib import Path

from linkdata.hrs import HRSInterviewData, ResidentialHistoryHRS
from .data_generators import create_survey_data


def create_survey_dataframe(n_people: int = 55):
    """
    Create fake survey/interview data matching the residential history IDs.

    Parameters
    ----------
    n_people : int
        Number of people to generate (default 55)

    Returns
    -------
    pd.DataFrame
        DataFrame with survey/interview data
    """
    import pandas as pd

    # Get data from standalone generator
    data_rows = create_survey_data(n_people)

    # Convert to DataFrame
    df = pd.DataFrame(data_rows)

    # Convert bcdate to datetime
    df["bcdate"] = pd.to_datetime(df["bcdate"])

    return df


@pytest.fixture
def fake_survey_file(tmp_path):
    """
    Create a fake survey/interview Stata file and return its path.

    Returns
    -------
    Path
        Path to the generated .dta file
    """
    df = create_survey_dataframe()
    file_path = tmp_path / "fake_survey_data.dta"
    df.to_stata(file_path, write_index=False)
    return file_path


@pytest.fixture
def fake_survey_file_persistent():
    """
    Create a persistent fake survey/interview Stata file in test_data directory.

    Returns
    -------
    Path
        Path to the generated .dta file
    """
    df = create_survey_dataframe()
    file_path = Path(__file__).parent / "test_data" / "fake_survey_data.dta"
    df.to_stata(file_path, write_index=False)
    return file_path


@pytest.fixture
def survey_data_hrs(fake_survey_file):
    """
    Create an HRSInterviewData instance from fake data.

    Returns
    -------
    HRSInterviewData
        Initialized instance with fake data
    """
    return HRSInterviewData(fake_survey_file)


@pytest.fixture
def survey_data_hrs_persistent(fake_survey_file_persistent):
    """
    Create a persistent HRSInterviewData instance from fake data.

    Returns
    -------
    HRSInterviewData
        Initialized instance with persistent fake data
    """
    return HRSInterviewData(fake_survey_file_persistent)


@pytest.fixture
def survey_with_residential_history(survey_data_hrs, residential_history_hrs):
    """
    Create HRSInterviewData with linked residential history.

    Returns
    -------
    HRSInterviewData
        Instance with residential history linked
    """
    survey_data_hrs.residential_hist = residential_history_hrs
    return survey_data_hrs


@pytest.fixture
def survey_with_residential_history_persistent(
    survey_data_hrs_persistent, residential_history_hrs_persistent
):
    """
    Create persistent HRSInterviewData with linked residential history.

    Returns
    -------
    HRSInterviewData
        Instance with residential history linked
    """
    survey_data_hrs_persistent.residential_hist = residential_history_hrs_persistent
    return survey_data_hrs_persistent


# Test cases
def test_survey_data_creation(survey_data_hrs):
    """Test that HRSInterviewData loads fake data correctly."""
    assert len(survey_data_hrs.df) > 0
    assert "hhidpn" in survey_data_hrs.df.columns
    assert "bcdate" in survey_data_hrs.df.columns
    assert survey_data_hrs.datecol == "bcdate"


def test_geoid_formatting(survey_data_hrs):
    """Test that GEOID columns are properly formatted."""
    geoid_cols = [c for c in survey_data_hrs.df.columns if "LINKCEN" in c]

    for col in geoid_cols:
        # Check that all GEOIDs are 11 characters
        geoid_lengths = survey_data_hrs.df[col].astype(str).str.len()
        assert geoid_lengths.eq(11).all()


def test_get_geoid_based_on_date(survey_with_residential_history):
    """Test the get_geoid_based_on_date method with residential history."""
    import pandas as pd

    # Create test dates matching the number of people in survey data
    n_people = len(survey_with_residential_history.df)
    test_dates = pd.Series([pd.Timestamp("2015-06-15")] * n_people)

    result = survey_with_residential_history.get_geoid_based_on_date(test_dates)

    assert len(result) == len(test_dates)
    assert result.dtype == "string"


def test_save_functionality(survey_data_hrs, tmp_path):
    """Test the save method."""
    import pandas as pd

    save_path = tmp_path / "test_save.dta"
    survey_data_hrs.save(save_path)

    assert save_path.exists()

    # Verify we can read it back
    loaded_df = pd.read_stata(save_path)
    assert len(loaded_df) == len(survey_data_hrs.df)
    # Stata files may include an index column, so check that all original columns are present
    for col in survey_data_hrs.df.columns:
        assert col in loaded_df.columns


def test_integration_with_residential_history(survey_with_residential_history):
    """Test integration between survey data and residential history."""
    import pandas as pd

    # Test that we can get GEOIDs based on dates
    test_date = pd.Timestamp("2015-06-15")
    n_people = len(survey_with_residential_history.df)
    test_dates = pd.Series([test_date] * n_people)

    result = survey_with_residential_history.get_geoid_based_on_date(test_dates)

    assert len(result) == n_people
    assert result.dtype == "string"


def test_column_validation():
    """Test that HRSInterviewData validates required columns."""
    import pandas as pd
    import tempfile
    from pathlib import Path

    # Create a temporary file with missing bcdate column
    df = pd.DataFrame({"hhidpn": [10000001, 10000002], "age": [65, 70]})

    with tempfile.NamedTemporaryFile(suffix=".dta", delete=False) as tmp_file:
        df.to_stata(tmp_file.name, write_index=False)
        tmp_path = Path(tmp_file.name)

    try:
        with pytest.raises(AssertionError, match="Date column `bcdate` not in data!"):
            HRSInterviewData(tmp_path, datecol="bcdate")
    finally:
        # Clean up
        tmp_path.unlink()

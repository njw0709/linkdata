"""
Test fixtures for ResidentialHistoryHRS class.

This module provides pytest fixtures that generate fake residential history data
in Stata format for testing the ResidentialHistoryHRS class functionality.
"""

import pytest
from pathlib import Path

from linkdata.hrs import ResidentialHistoryHRS
from .data_generators import create_residential_history_data


def create_residential_history_dataframe(n_people: int = 55):
    """
    Create fake residential history data with varied move patterns.

    Parameters
    ----------
    n_people : int
        Number of people to generate (default 55)

    Returns
    -------
    pd.DataFrame
        DataFrame with residential history data
    """
    import pandas as pd

    # Get data from standalone generator
    data_rows = create_residential_history_data(n_people)

    # Convert to DataFrame
    df = pd.DataFrame(data_rows)

    # Convert appropriate columns to proper types
    df["mvyear"] = pd.to_numeric(df["mvyear"], errors="coerce")
    df["mvmonth"] = pd.to_numeric(df["mvmonth"], errors="coerce")
    df["year"] = pd.to_numeric(df["year"], errors="coerce")

    # Convert trmove_tr to proper format for Stata export
    df["trmove_tr"] = df["trmove_tr"].astype(str)

    return df


@pytest.fixture
def fake_residential_history_file(tmp_path):
    """
    Create a fake residential history Stata file and return its path.

    Returns
    -------
    Path
        Path to the generated .dta file
    """
    df = create_residential_history_dataframe()
    file_path = tmp_path / "fake_residential_history.dta"
    df.to_stata(file_path, write_index=False)
    return file_path


@pytest.fixture
def fake_residential_history_file_persistent():
    """
    Create a persistent fake residential history Stata file in test_data directory.

    Returns
    -------
    Path
        Path to the generated .dta file
    """
    df = create_residential_history_dataframe()
    file_path = Path(__file__).parent / "test_data" / "fake_residential_history.dta"
    df.to_stata(file_path, write_index=False)
    return file_path


@pytest.fixture
def residential_history_hrs(fake_residential_history_file):
    """
    Create a ResidentialHistoryHRS instance from fake data.

    Returns
    -------
    ResidentialHistoryHRS
        Initialized instance with fake data
    """
    return ResidentialHistoryHRS(
        fake_residential_history_file, first_tract_mark="999.0"
    )


@pytest.fixture
def residential_history_hrs_persistent(fake_residential_history_file_persistent):
    """
    Create a persistent ResidentialHistoryHRS instance from fake data.

    Returns
    -------
    ResidentialHistoryHRS
        Initialized instance with persistent fake data
    """
    return ResidentialHistoryHRS(
        fake_residential_history_file_persistent, first_tract_mark="999.0"
    )


# Test cases
def test_residential_history_creation(residential_history_hrs):
    """Test that ResidentialHistoryHRS loads fake data correctly."""
    assert len(residential_history_hrs.df) > 0
    assert "hhidpn" in residential_history_hrs.df.columns
    assert "trmove_tr" in residential_history_hrs.df.columns
    assert "LINKCEN2010" in residential_history_hrs.df.columns


def test_move_info_parsing(residential_history_hrs):
    """Test that move info is parsed correctly."""
    move_info = residential_history_hrs._move_info

    # Should have entries for all people
    assert len(move_info) > 0

    # Check structure of move info
    for hhidpn, (dates, geoids) in move_info.items():
        assert isinstance(dates, list)
        assert isinstance(geoids, list)
        assert len(dates) == len(geoids)
        assert len(dates) >= 1  # At least first tract


def test_geoid_lookup(residential_history_hrs):
    """Test GEOID lookup functionality."""
    import pandas as pd

    # Get a person with moves
    move_info = residential_history_hrs._move_info
    test_person = None
    for hhidpn, (dates, geoids) in move_info.items():
        if len(dates) > 1:  # Person with moves
            test_person = hhidpn
            break

    if test_person:
        dates, geoids = move_info[test_person]

        # Test lookup for first date
        result = residential_history_hrs._find_geoid_for_date(dates[0], dates, geoids)
        assert result == geoids[0]

        # Test lookup for date before first move
        early_date = dates[0] - pd.Timedelta(days=30)
        result = residential_history_hrs._find_geoid_for_date(early_date, dates, geoids)
        assert result is None


def test_create_geoid_based_on_date(residential_history_hrs):
    """Test the create_geoid_based_on_date method."""
    import pandas as pd

    # Create test data
    hhidpns = list(residential_history_hrs._move_info.keys())[:5]  # First 5 people
    test_dates = pd.Series([pd.Timestamp("2015-06-15")] * len(hhidpns))
    hhidpn_series = pd.Series(hhidpns)

    result = residential_history_hrs.create_geoid_based_on_date(
        hhidpn_series, test_dates
    )

    assert len(result) == len(hhidpns)
    assert result.dtype == "string"

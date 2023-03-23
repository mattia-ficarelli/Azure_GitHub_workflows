from __future__ import annotations

import argparse
import logging
import pathlib
from datetime import datetime
from typing import TYPE_CHECKING
from typing import TypeAlias

import pandas as pd

if TYPE_CHECKING:
    from collections.abc import Callable
    from collections.abc import Sequence

StrPath: TypeAlias = str | pathlib.Path


logger = logging.getLogger(__name__)


class DataProcessingError(Exception):
    """Base Exception for data processing."""


def _is_date_in_source(source_folder: pathlib.Path) -> Callable[[str], bool]:
    def _is_date(date: str) -> bool:
        try:
            datetime.strptime(date, "%Y-%m-%d")  # noqa: DTZ007
            return True
        except ValueError:
            logger.warning(
                "Expected folders within %s to be a date with format %%Y-%%m-%%d, "
                "ignoring %s",
                source_folder,
                source_folder / date,
            )
            return False

    return _is_date


def process(source_folder: StrPath, destination_file: StrPath) -> None:
    """Process an excel file for inclusion to the data lake.

    Parameters
    ----------
        source_folder : str | pathlib.Path
            Folder to search for files in.
        destination_file : str | pathlib.Path
            Target name of processed csv.
    """
    source_folder = pathlib.Path(source_folder)
    destination_file = pathlib.Path(destination_file)

    is_date = _is_date_in_source(source_folder)

    latest_folder = max(
        path.name for path in source_folder.glob("*") if is_date(path.name)
    )
    input_file_name = list((source_folder / latest_folder).glob("*"))[0]

    try:
        excel_df = pd.read_excel(
            input_file_name,
            usecols=["Gym ID", "LFL Status"],
            dtype="string",
        ).rename(columns={"Gym ID": "center_id", "LFL Status": "lfl_status"})
    except ValueError as exc:
        msg = f"{input_file_name} is not an excel file"
        raise DataProcessingError(msg) from exc

    excel_df = (
        excel_df[
            excel_df["center_id"].str.isdigit()
            & excel_df["lfl_status"].isin(["LFL", "Non-LFL"])
        ]
        .assign(
            center_id=lambda df: df["center_id"].astype("int16"),
            Date=latest_folder,
        )
        .sort_values(["center_id"])
    )

    excel_df.to_csv(destination_file, index=False)


def main(argv: Sequence[str] | None = None) -> int:
    """Entrypoint for CLI tool."""
    parser = argparse.ArgumentParser()

    parser.add_argument("source_folder")
    parser.add_argument("destination_file")

    args = parser.parse_args(argv)
    try:
        process(args.source_folder, args.destination_file)
    except DataProcessingError as exc:
        logger.exception(exc)
        return 1

    return 0

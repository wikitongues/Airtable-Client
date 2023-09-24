import urllib.parse
from abc import ABC, abstractmethod
from enum import Enum
from typing import List, Optional

import requests
from requests import Response

from .airtable_connection_info import AirtableConnectionInfo
from .airtable_table_info import AirtableTableInfo


class CellFormat(Enum):
    JSON = "json"
    STRING = "string"


class IAirtableHttpClient(ABC):
    """
    Airtable Http Client Interface

    Args:
        ABC
    """

    @abstractmethod
    def list_records(
        self,
        page_size: Optional[int] = 100,
        offset: Optional[str] = None,
        max_records: Optional[int] = None,
        cell_format: Optional[CellFormat] = None,
        time_zone: Optional[str] = None,
        user_locale: Optional[str] = None,
    ) -> Response:
        """
        List records

        Args:
            page_size (int, optional): Page size. Defaults to 100.
            offset (str, optional): Offset for pagination. Defaults to None.
            max_records (int, optional): Max records. Defaults to None.
        """
        pass

    @abstractmethod
    def get_record(self, id: str) -> Response:
        """
        Get record

        Args:
            id (str): Id of record
        """
        pass

    @abstractmethod
    def get_records_by_fields(self, fields: dict) -> Response:
        """
        Get any records matching the given fields

        Args:
            fields (dict): Dictionary of fields
        """
        pass

    @abstractmethod
    def create_record(self, fields: dict) -> Response:
        """
        Create record

        Args:
            fields (dict): Dictionary of fields
        """
        pass


class AirtableHttpClient(IAirtableHttpClient):
    """
    Http client for accessing an Airtable base

    Args:
        IAirtableHttpClient
    """

    _base_url = "https://api.airtable.com/v0"

    def __init__(self, connection_info: AirtableConnectionInfo, table_info: AirtableTableInfo) -> None:
        """
        Construct AirtableHttpClient

        Args:
            connection_info (AirtableConnectionInfo)
            table_info (AirtableTableInfo)
        """

        self._route = "/".join([self._base_url, connection_info.base_id, table_info.name])

        self._headers = {"Authorization": f"Bearer {connection_info.api_key}"}

        self._id_column = table_info.id_column

    @staticmethod
    def _handle_cell_format_params(
        params: List[str],
        cell_format: Optional[CellFormat] = None,
        time_zone: Optional[str] = None,
        user_locale: Optional[str] = None,
    ) -> None:
        assert (
            cell_format != CellFormat.STRING or time_zone and user_locale
        ), "time_zone and user_locale are required if cell_format is string"

        if cell_format == CellFormat.STRING:
            params.append(f"cellFormat={cell_format.value}")
            params.append(f"timeZone={time_zone}")
            params.append(f"userLocale={user_locale}")

    def list_records(
        self,
        *,
        page_size: Optional[int] = 100,
        offset: Optional[str] = None,
        max_records: Optional[int] = None,
        cell_format: Optional[CellFormat] = None,
        time_zone: Optional[str] = None,
        user_locale: Optional[str] = None,
    ) -> Response:
        params = [f"maxRecords={max_records}"]

        if page_size is not None:
            params.append(f"pageSize={page_size}")

        if offset is not None:
            params.append(f"offset={offset}")

        AirtableHttpClient._handle_cell_format_params(params, cell_format, time_zone, user_locale)

        url = f'{self._route}?{"&".join(params)}'

        return requests.get(url, headers=self._headers)

    def get_record(
        self,
        id: str,
        *,
        cell_format: Optional[CellFormat] = None,
        time_zone: Optional[str] = None,
        user_locale: Optional[str] = None,
    ) -> Response:
        formula = urllib.parse.quote_plus(f"FIND('{id}', {{{self._id_column}}}) != 0")
        params = [f"filterByFormula={formula}"]
        AirtableHttpClient._handle_cell_format_params(params, cell_format, time_zone, user_locale)

        url = f'{self._route}?{"&".join(params)}'

        return requests.get(url, headers=self._headers)

    def get_records_by_fields(
        self,
        fields: dict,
        *,
        cell_format: Optional[CellFormat] = None,
        time_zone: Optional[str] = None,
        user_locale: Optional[str] = None,
    ) -> Response:
        formula = "AND("
        formula += ",".join(["{" + key + "}='" + fields[key] + "'" for key in sorted(fields) if fields[key]])
        formula += ")"
        formula = urllib.parse.quote_plus(formula)
        params = [f"filterByFormula={formula}"]
        AirtableHttpClient._handle_cell_format_params(params, cell_format, time_zone, user_locale)

        url = f'{self._route}?{"&".join(params)}'

        return requests.get(url, headers=self._headers)

    def create_record(self, fields: dict) -> Response:
        json_obj = {"records": [{"fields": fields}]}

        headers = {**self._headers, "Content-Type": "application/json"}

        return requests.post(self._route, json=json_obj, headers=headers)

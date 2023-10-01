import json
import urllib.parse
from enum import Enum
from typing import Any, Dict, List, Optional

import requests
from requests import Response

from .airtable_connection_info import AirtableConnectionInfo
from .airtable_table_info import AirtableTableInfo


class CellFormat(Enum):
    JSON = "json"
    STRING = "string"


class AirtableHttpClientError(Exception):
    pass


class AirtableApiError(AirtableHttpClientError):
    pass


class AirtableBadResponseError(AirtableHttpClientError):
    pass


class AirtableHttpClient:
    """
    Http client for accessing an Airtable base
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
    def _check_response(response: Response) -> None:
        if response.status_code != 200:
            raise AirtableApiError

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
    ) -> Dict[str, Any]:
        formula = urllib.parse.quote_plus(f"FIND('{id}', {{{self._id_column}}}) != 0")
        params = [f"filterByFormula={formula}"]
        AirtableHttpClient._handle_cell_format_params(params, cell_format, time_zone, user_locale)

        url = f'{self._route}?{"&".join(params)}'

        response = requests.get(url, headers=self._headers)

        AirtableHttpClient._check_response(response)

        j = json.loads(response.text)
        if (
            "records" not in j
            or type(j["records"]) != list
            or len(j["records"]) != 1
            or "fields" not in j["records"][0]
        ):
            raise AirtableBadResponseError

        return j["records"][0]["fields"]

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

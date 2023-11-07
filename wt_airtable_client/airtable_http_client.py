import urllib.parse
from enum import Enum
from typing import Any, Dict, Iterable, List, NamedTuple, Optional

import requests
from requests import Response

from .airtable_connection_info import AirtableConnectionInfo
from .airtable_table_info import AirtableTableInfo


class CellFormat(Enum):
    JSON = "json"
    STRING = "string"


class AirtableHttpClientError(Exception):
    pass


class AirtableBadResponseError(AirtableHttpClientError):
    pass


class AirtableRecord(NamedTuple):
    id: str
    fields: Dict[str, Any]
    # TODO created_time

    @classmethod
    def from_dict(cls, d) -> "AirtableRecord":
        return cls(id=d["id"], fields=d["fields"])


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
    def _unpack_records(response: Response) -> Iterable[AirtableRecord]:
        j = response.json()
        if "records" not in j or type(j["records"]) != list:
            raise AirtableBadResponseError
        try:
            yield from (AirtableRecord.from_dict(d) for d in j["records"])
        except KeyError:
            raise AirtableBadResponseError

    @staticmethod
    def _unpack_single_record(response: Response) -> AirtableRecord:
        j = response.json()
        if "records" not in j or type(j["records"]) != list or len(j["records"]) != 1:
            raise AirtableBadResponseError
        try:
            return AirtableRecord.from_dict(j["records"][0])
        except KeyError:
            raise AirtableBadResponseError

    @staticmethod
    def _handle_pagination_params(
        params: List[str],
        page_size: Optional[int] = 100,
        offset: Optional[str] = None,
        max_records: Optional[int] = None,
    ):
        if max_records is not None:
            params.append(f"maxRecords={max_records}")

        if page_size is not None:
            params.append(f"pageSize={page_size}")

        if offset is not None:
            params.append(f"offset={offset}")

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
        page_size: Optional[int] = None,
        offset: Optional[str] = None,
        max_records: Optional[int] = None,
        cell_format: Optional[CellFormat] = None,
        time_zone: Optional[str] = None,
        user_locale: Optional[str] = None,
    ):
        params = []
        AirtableHttpClient._handle_pagination_params(params, page_size, offset, max_records)
        AirtableHttpClient._handle_cell_format_params(params, cell_format, time_zone, user_locale)

        url = f'{self._route}?{"&".join(params)}'

        response = requests.get(url, headers=self._headers)

        response.raise_for_status()

        yield from AirtableHttpClient._unpack_records(response)

        j = response.json()
        if "offset" in j:
            yield from self.list_records(
                page_size=page_size,
                offset=j["offset"],
                max_records=max_records,
                cell_format=cell_format,
                time_zone=time_zone,
                user_locale=user_locale,
            )

    def get_record(
        self,
        id: str,
        *,
        cell_format: Optional[CellFormat] = None,
        time_zone: Optional[str] = None,
        user_locale: Optional[str] = None,
    ) -> AirtableRecord:
        formula = urllib.parse.quote_plus(f"FIND('{id}', {{{self._id_column}}}) != 0")
        params = [f"filterByFormula={formula}"]
        AirtableHttpClient._handle_cell_format_params(params, cell_format, time_zone, user_locale)

        url = f'{self._route}?{"&".join(params)}'

        response = requests.get(url, headers=self._headers)

        response.raise_for_status()

        return AirtableHttpClient._unpack_single_record(response)

    def get_records_by_fields(
        self,
        fields: dict,
        *,
        page_size: Optional[int] = None,
        offset: Optional[str] = None,
        max_records: Optional[int] = None,
        cell_format: Optional[CellFormat] = None,
        time_zone: Optional[str] = None,
        user_locale: Optional[str] = None,
    ) -> Iterable[AirtableRecord]:
        formula = "AND("
        formula += ",".join(["{" + key + "}='" + fields[key] + "'" for key in sorted(fields) if fields[key]])
        formula += ")"
        formula = urllib.parse.quote_plus(formula)
        params = [f"filterByFormula={formula}"]
        AirtableHttpClient._handle_pagination_params(params, page_size, offset, max_records)
        AirtableHttpClient._handle_cell_format_params(params, cell_format, time_zone, user_locale)

        url = f'{self._route}?{"&".join(params)}'

        response = requests.get(url, headers=self._headers)

        response.raise_for_status()

        yield from AirtableHttpClient._unpack_records(response)

        j = response.json()
        if "offset" in j:
            yield from self.get_records_by_fields(
                fields,
                page_size=page_size,
                offset=j["offset"],
                max_records=max_records,
                cell_format=cell_format,
                time_zone=time_zone,
                user_locale=user_locale,
            )

    def create_record(self, fields: dict) -> AirtableRecord:
        json_obj = {"records": [{"fields": fields}]}

        headers = {**self._headers, "Content-Type": "application/json"}

        response = requests.post(self._route, json=json_obj, headers=headers)

        response.raise_for_status()

        return AirtableHttpClient._unpack_single_record(response)

    def update_record(self, id: str, fields: dict) -> AirtableRecord:
        json_obj = {"fields": fields}

        headers = {**self._headers, "Content-Type": "application/json"}

        response = requests.put(f"{self._route}/{id}", json=json_obj, headers=headers)

        response.raise_for_status()

        return AirtableRecord.from_dict(response.json())

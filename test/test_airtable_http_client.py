import json

import pytest
import responses

from wt_airtable_client import (
    AirtableApiError,
    AirtableBadResponseError,
    AirtableConnectionInfo,
    AirtableHttpClient,
    AirtableTableInfo,
)

BASE_ID = "base_id"
API_KEY = "api_key"
TABLE = "MyTable"
ID_COLUMN = "Identifier"

CONNECTION_INFO = AirtableConnectionInfo(BASE_ID, API_KEY)
TABLE_INFO = AirtableTableInfo(TABLE, ID_COLUMN)


class TestAirtableHttpClient:
    @pytest.fixture
    def client(self):
        return AirtableHttpClient(CONNECTION_INFO, TABLE_INFO)

    @responses.activate
    def test_list_records(self, client):
        text = "expected text"
        page_size = 3
        offset = "rec123"
        max_records = 10
        url = (
            f"https://api.airtable.com/v0/{BASE_ID}/{TABLE}?"
            f"maxRecords={max_records}&pageSize={page_size}&offset={offset}"
        )

        def callback(request):
            if request.headers["Authorization"] != f"Bearer {API_KEY}":
                return (401, {}, None)

            return (200, {}, text)

        responses.add_callback(responses.GET, url, callback=callback)

        result = client.list_records(page_size=page_size, offset=offset, max_records=max_records)

        assert result.text == text

    @responses.activate
    def test_get_record(self, client):
        fields = {"Field 1": "Value 1"}
        id = "id123"
        url = (
            f"https://api.airtable.com/v0/{BASE_ID}/{TABLE}?filterByFormula="
            f"FIND%28%27{id}%27%2C+%7B{ID_COLUMN}%7D%29+%21%3D+0"
        )

        def callback(request):
            if request.url != url:
                return (404, {}, None)

            if request.headers["Authorization"] != f"Bearer {API_KEY}":
                return (401, {}, None)

            return (
                200,
                {},
                json.dumps(
                    {
                        "records": [{"fields": fields}],
                    }
                ),
            )

        responses.add_callback(responses.GET, url, callback=callback)

        result = client.get_record(id)

        assert result == fields

    @responses.activate
    def test_get_record__error(self, client):
        id = "id123"
        url = (
            f"https://api.airtable.com/v0/{BASE_ID}/{TABLE}?filterByFormula="
            f"FIND%28%27{id}%27%2C+%7B{ID_COLUMN}%7D%29+%21%3D+0"
        )
        responses.add(responses.GET, url, status=404)

        with pytest.raises(AirtableApiError):
            client.get_record(id)

    @responses.activate
    @pytest.mark.parametrize(
        "j",
        [
            {},
            {"records": []},
            {"records": {}},
            {"records": [{}]},
            {"records": [{"fields": {}}, {"fields": {}}]},
        ],
    )
    def test_get_record__bad_response(self, j, client):
        id = "id123"
        url = (
            f"https://api.airtable.com/v0/{BASE_ID}/{TABLE}?filterByFormula="
            f"FIND%28%27{id}%27%2C+%7B{ID_COLUMN}%7D%29+%21%3D+0"
        )
        responses.add(responses.GET, url, json=j, status=200)

        with pytest.raises(AirtableBadResponseError):
            client.get_record(id)

    @responses.activate
    def test_get_records_by_fields(self, client):
        text = "expected text"
        iso = "sah"
        resource_url = "http://www.baayaga.narod.ru"
        fields = {"Subject [ISO Code]": iso, "Coverage [Web: Link]": resource_url}
        url = (
            f"https://api.airtable.com/v0/{BASE_ID}/{TABLE}?filterByFormula="
            "AND%28%7BCoverage+%5BWeb%3A+Link%5D%7D%3D%27http%3A%2F%2Fwww.baayaga.narod.ru%27%2C%7BSubject+%5BISO+Code%5D%7D%3D%27sah%27%29"  # noqa: E501
        )

        def callback(request):
            if request.url != url:
                return (404, {}, None)

            if request.headers["Authorization"] != f"Bearer {API_KEY}":
                return (401, {}, None)

            return (200, {}, text)

        responses.add_callback(responses.GET, url, callback=callback)

        result = client.get_records_by_fields(fields)

        assert result.text == text

    @responses.activate
    def test_get_records_by_fields__null_value(self, client):
        text = "expected text"
        resource_url = "http://www.baayaga.narod.ru"
        fields = {"Subject [ISO Code]": None, "Coverage [Web: Link]": resource_url}
        url = (
            f"https://api.airtable.com/v0/{BASE_ID}/{TABLE}?filterByFormula="
            "AND%28%7BCoverage+%5BWeb%3A+Link%5D%7D%3D%27http%3A%2F%2Fwww.baayaga.narod.ru%27%29"
        )

        def callback(request):
            if request.url != url:
                return (404, {}, None)

            if request.headers["Authorization"] != f"Bearer {API_KEY}":
                return (401, {}, None)

            return (200, {}, text)

        responses.add_callback(responses.GET, url, callback=callback)

        result = client.get_records_by_fields(fields)

        assert result.text == text

    @responses.activate
    def test_create_record(self, client):
        text = "expected text"
        url = f"https://api.airtable.com/v0/{BASE_ID}/{TABLE}"
        fields = {"a": "a", "b": "b", "c": "c"}

        def callback(request):
            if request.headers["Authorization"] != f"Bearer {API_KEY}":
                return (401, {}, None)

            json_obj = json.loads(request.body)

            if type(json_obj["records"]) != list:
                return (400, {}, None)

            if len(json_obj["records"]) != 1:
                return (400, {}, None)

            if type(json_obj["records"][0]["fields"]) != dict:
                return (400, {}, None)

            return (200, {}, text)

        responses.add_callback(responses.POST, url, callback=callback)

        result = client.create_record(fields)

        assert result.text == text

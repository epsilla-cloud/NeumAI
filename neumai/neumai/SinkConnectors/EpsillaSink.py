from neumai.Shared.NeumSinkInfo import NeumSinkInfo
from neumai.Shared.NeumVector import NeumVector
from neumai.Shared.NeumSearch import NeumSearchResult
from neumai.Shared.Exceptions import (
    EpsillaConnectionException,
    EpsillaInsertionException,
    EpsillaIndexInfoException,
    EpsillaQueryException
)
from neumai.SinkConnectors.SinkConnector import SinkConnector
from typing import List
from pydantic import Field
from re import findall

from pyepsilla import cloud


class EpsillaSink(SinkConnector):

    """
    Epsilla sink

    A sink connector for Epsilla, designed to facilitate data output into a
    Epsilla storage system. For details about Epsilla, refer to
    https://epsilla-inc.gitbook.io/epsilladb/quick-start/epsilla-cloud.


    Attributes:
    -----------
    project_id: str
        Project to use on Epsilla Cloud.
    api_key: str
        API key for accessing the Epsilla Cloud service.
    db_id: str
        Database to use on Epsilla Cloud.
    table_name: str
        Name of Epsilla table to use


    Example usage:
        db = EpsillaSink(
          project_id="your_epsilla_project_id",
          api_key="your_epsilla_api_key",
          db_id="your_epsilla_db_id",
          table_name="your_table"
        )
        db.store(neum_vectors)
        db.search(query)
    """

    project_id: str = Field(..., description="Project to use on Epsilla Cloud")
    api_key: str = Field(..., description="API key for accessing the Epsilla Cloud service")
    db_id: str = Field(..., description="Database to use on Epsilla Cloud")
    table_name: str = Field(..., description="Name of Epsilla table to use")

    @property
    def sink_name(self) -> str:
        return "EpsillaSink"

    @property
    def required_properties(self) -> List[str]:
        return ['project_id', 'api_key', 'db_id', 'table_name']

    @property
    def optional_properties(self) -> List[str]:
        return []

    def validation(self) -> bool:
        """config_validation connector setup"""
        try:
            client = cloud.Client(project_id=self.project_id, api_key=self.api_key)
            client.validate()
        except Exception as e:
            raise EpsillaConnectionException(f"Epsilla Cloud connection couldn't be initialized. See exception: {e}")
        return True

    def _get_db(self):
        client = cloud.Client(project_id=self.project_id, api_key=self.api_key)
        return client.vectordb(self.db_id)

    def _extract_numbers(input_string):
        return [int(match) for match in findall(r'\d+', input_string)]

    def store(self, vectors_to_store: List[NeumVector]) -> int:
        db = self._get_db()
        table_name = self.table_name

        status_code, response = db.list_tables()
        if status_code != 200:
            raise EpsillaInsertionException("Epsilla storing failed. Try later")
        if table_name not in response["result"]:
            dimensions = len(vectors_to_store[0].vector)
            status_code, response = db.create_table(table_name=table_name, table_fields=[
                {"name": "id", "dataType": "STRING"},
                {"name": "vector", "dataType": "VECTOR_FLOAT", "dimensions": dimensions},
                {"name": "metadata", "dataType": "JSON"},
                {"name": "file_entry_id", "dataType": "STRING"}
            ])
        if status_code != 200:
            raise EpsillaInsertionException("Epsilla storing failed. Try later")

        datas = []
        for vec in vectors_to_store:
            data = {
                "id": vec.id,
                "vector": vec.vector,
                "metadata": vec.metadata if vec.metadata else {},
                "file_entry_id": vec.metadata["_file_entry_id"] if vec.metadata["_file_entry_id"] else ""
            }
            datas.append(data)

        status_code, response = db.insert(table_name=table_name, records=datas)
        if status_code != 200:
            raise EpsillaInsertionException("Epsilla storing failed. Try later")
        return self._extract_numbers(response["message"])

    def search(self, vector: List[float],
               number_of_results: int, filter: dict = {}) -> List[NeumSearchResult]:
        # Epsilla hasn't supported filtering by metadata yet. Will support soon.
        db = self._get_db()

        # filter_str = ""
        # filters = []
        # if filter:
        #     for k, v in filter.items():
        #         filters.append(f"{k} = {v}")
        #     filter_str = " AND ".join(filters)
        status_code, response = db.query(
            table_name=self.table_name,
            query_field="vector",
            query_vector=vector,
            limit=number_of_results,
            with_distance=True
        )
        if status_code != 200:
            raise EpsillaQueryException(f"Failed to query Epsilla. Exception - {response['message']}")

        matches = []
        for result in response["result"]:
            matches.append(
                NeumSearchResult(
                    id=result["id"],
                    vector=result["vector"],
                    metadata=result["metadata"],
                    score=result["@distance"]
                )
            )
        return matches

    def info(self) -> NeumSinkInfo:
        db = self._get_db()
        status_code, response = db.get(table_name=self.table_name)
        if status_code != 200:
            raise EpsillaIndexInfoException(
                f"Failed to get information from Epsilla. Exception - {response['message']}"
            )

        return (NeumSinkInfo(number_vectors_stored=len(response["result"])))

    def delete_vectors_with_file_id(self, file_id: str) -> bool:
        db = self._get_db()

        status_code, response = db.delete(table_name=self.table_name, filter=f"file_entry_id = {file_id}")
        if status_code != 200:
            raise EpsillaInsertionException("Epsilla deletion failed. Try later")
        return True

"""Azure CosmosDB Memory History."""
from __future__ import annotations

import logging
from types import TracebackType
from typing import TYPE_CHECKING, Any, List, Optional, Type, Union

from pydantic import root_validator

from langchain.schema import (
    BaseChatMessageHistory,
    BaseMessage,
    messages_from_dict,
    messages_to_dict,
)

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from azure.cosmos import ContainerProxy, CosmosClient
    from azure.cosmos.aio import (
        ContainerProxy as AsyncContainerProxy,
        CosmosClient as AsyncCosmosClient,
    )


class CosmosDBChatMessageHistory(BaseChatMessageHistory):
    """Chat history backed by Azure CosmosDB."""

    def __init__(
        self,
        cosmos_endpoint: str,
        cosmos_database: str,
        cosmos_container: str,
        session_id: str,
        user_id: str,
        credential: Any = None,
        connection_string: Optional[str] = None,
        ttl: Optional[int] = None,
        cosmos_client_kwargs: Optional[dict] = None,
        messages: Optional[List[BaseMessage]] = None,
        async_mode: Optional[bool] = False,
        direct_io_mode: Optional[bool] = True,
    ):
        """
        Initializes a new instance of the CosmosDBChatMessageHistory class.

        Make sure to call prepare_cosmos or use the context manager to make
        sure your database is ready.

        Either a credential or a connection string must be provided.

        :param cosmos_endpoint: The connection endpoint for the Azure Cosmos DB account.
        :param cosmos_database: The name of the database to use.
        :param cosmos_container: The name of the container to use.
        :param session_id: The session ID to use, can be overwritten while loading.
        :param user_id: The user ID to use, can be overwritten while loading.
        :param credential: The credential to use to authenticate to Azure Cosmos DB.
        :param connection_string: The connection string to use to authenticate.
        :param ttl: The time to live (in seconds) to use for documents in the container.
        :param cosmos_client_kwargs: Additional kwargs to pass to the CosmosClient.
        :param messages: The messages to load into the history.
        :param async_mode: Whether to use async mode.
        :param direct_io_mode: Whether to (a)sync the history after every message.
        """
        super().__init__(messages, async_mode, direct_io_mode)
        self.cosmos_endpoint = cosmos_endpoint
        self.cosmos_database = cosmos_database
        self.cosmos_container = cosmos_container
        self.credential = credential
        self.conn_string = connection_string
        self.session_id = session_id
        self.user_id = user_id
        self.ttl = ttl
        self._cosmos_client_kwargs = cosmos_client_kwargs or {}

        self._client: Optional[Union[CosmosClient, AsyncCosmosClient]] = None
        self._container: Optional[Union[ContainerProxy, AsyncContainerProxy]] = None

    @root_validator
    def check_fields(cls, values):
        """Check that at least one of the fields credentials and connection_string is filled."""
        if "credential" in values:
            if "connection_string" in values:
                logger.warning(
                    "Both credential and connection_string are set, credential will be used."
                )
            return values
        if "connection_string" in values:
            return values
        raise ValueError("Either credential or connection_string must be set.")

    def add_message(self, message: BaseMessage) -> None:
        """Add a self-created message to the store"""
        self.messages.append(message)
        if self.direct_io_mode:
            self.sync()

    def clear(self) -> None:
        """Clear session memory from this memory and cosmos."""
        self.messages = []
        if self._container:
            self._container.delete_item(
                item=self.session_id, partition_key=self.user_id
            )

    def setup(self) -> None:
        """Prepare the CosmosDB client.

        Use this function or the context manager to make sure your database is ready.
        """
        if self.async_mode:
            raise RuntimeError("Use asetup for async mode.")
        try:
            from azure.cosmos import (  # pylint: disable=import-outside-toplevel # noqa: E501
                CosmosClient,
                PartitionKey,
            )
            from azure.cosmos.exceptions import (  # pylint: disable=import-outside-toplevel # noqa: E501
                CosmosHttpResponseError,
            )
        except ImportError as exc:
            raise ImportError(
                "You must install the azure-cosmos package to use the CosmosDBChatMessageHistory."  # noqa: E501
            ) from exc
        if self.credential:
            self._client = CosmosClient(
                url=self.cosmos_endpoint,
                credential=self.credential,
                **self._cosmos_client_kwargs,
            )
        else:
            self._client = CosmosClient.from_connection_string(
                conn_str=self.conn_string,
                **self._cosmos_client_kwargs,
            )
        database = self._client.create_database_if_not_exists(self.cosmos_database)
        self._container = database.create_container_if_not_exists(
            self.cosmos_container,
            partition_key=PartitionKey("/user_id"),
            default_ttl=self.ttl,
        )
        try:
            item = self._container.read_item(
                item=self.session_id, partition_key=self.user_id
            )
        except CosmosHttpResponseError:
            logger.info("no session found")
            return
        if "messages" in item and len(item["messages"]) > 0:
            self.messages = messages_from_dict(item["messages"])

    def sync(self) -> None:
        """Update the cosmosdb item."""
        if not self._container:
            raise ValueError("Container not initialized")
        self._container.upsert_item(
            body={
                "id": self.session_id,
                "user_id": self.user_id,
                "messages": messages_to_dict(self.messages),
            }
        )

    def __enter__(self) -> "CosmosDBChatMessageHistory":
        """Context manager entry point."""
        if self.async_mode:
            raise RuntimeError("Use __aenter__ or async with for async mode.")
        self._client.__enter__()
        self.setup()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        """Context manager exit"""
        self.sync()
        self._client.__exit__(exc_type, exc_val, traceback)

    async def aadd_message(self, message: BaseMessage) -> None:
        """Add a self-created message to the store"""
        self.messages.append(message)
        if self.direct_io_mode:
            await self.a_sync()

    async def aclear(self) -> None:
        """Clear session memory from this memory and cosmos."""
        self.messages = []
        if self._container:
            await self._container.delete_item(
                item=self.session_id, partition_key=self.user_id
            )

    async def asetup(self) -> None:
        """Prepare the CosmosDB client.

        Use this function or the context manager to make sure your database is ready.
        """
        if not self.async_mode:
            raise RuntimeError("Use setup for sync mode.")
        try:
            from azure.cosmos.aio import (  # pylint: disable=import-outside-toplevel # noqa: E501
                CosmosClient,
            )
            from azure.cosmos import (  # pylint: disable=import-outside-toplevel # noqa: E501
                PartitionKey,
            )
            from azure.cosmos.exceptions import (  # pylint: disable=import-outside-toplevel # noqa: E501
                CosmosHttpResponseError,
            )
        except ImportError as exc:
            raise ImportError(
                "You must install the azure-cosmos package to use the CosmosDBChatMessageHistory."  # noqa: E501
            ) from exc
        if self.credential:
            self._client = CosmosClient(
                url=self.cosmos_endpoint,
                credential=self.credential,
                **self._cosmos_client_kwargs,
            )
        else:
            self._client = CosmosClient.from_connection_string(
                conn_str=self.conn_string,
                **self._cosmos_client_kwargs,
            )
        database = await self._client.create_database_if_not_exists(
            self.cosmos_database
        )
        self._container = await database.create_container_if_not_exists(
            self.cosmos_container,
            partition_key=PartitionKey("/user_id"),
            default_ttl=self.ttl,
        )
        try:
            item = await self._container.read_item(
                item=self.session_id, partition_key=self.user_id
            )
        except CosmosHttpResponseError:
            logger.info("no session found")
            return
        if "messages" in item and len(item["messages"]) > 0:
            self.messages = messages_from_dict(item["messages"])

    async def a_sync(self) -> None:
        if not self._container:
            raise ValueError("Container not initialized")
        await self._container.upsert_item(
            body={
                "id": self.session_id,
                "user_id": self.user_id,
                "messages": messages_to_dict(self.messages),
            }
        )

    async def __aenter__(self) -> "CosmosDBChatMessageHistory":
        """Async context manager entry point."""
        if not self.async_mode:
            raise RuntimeError("Use __enter__ or with for sync mode.")
        await self._client.__aenter__()
        await self.asetup()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        """Async context manager exit"""
        await self.a_sync()
        await self._client.__aexit__(exc_type, exc_val, traceback)

# flake8: noqa
"""Tools for interacting with a Power BI dataset."""
from pydantic import BaseModel, Extra, Field, validator

from langchain.chains.llm import LLMChain
from langchain.llms.openai import OpenAI
from langchain.prompts import PromptTemplate
from langchain.powerbi import PowerBIDataset
from langchain.tools.base import BaseTool
from langchain.tools.powerbi.prompt import QUERY_CHECKER


class BasePowerBIDatabaseTool(BaseModel):
    """Base tool for interacting with a Power BI Dataset."""

    powerbi: PowerBIDataset = Field(exclude=True)

    # Override BaseTool.Config to appease mypy
    # See https://github.com/pydantic/pydantic/issues/4173
    class Config(BaseTool.Config):
        """Configuration for this pydantic object."""

        arbitrary_types_allowed = True
        extra = Extra.forbid


class QueryPowerBITool(BasePowerBIDatabaseTool, BaseTool):
    """Tool for querying a Power BI Dataset."""

    name = "query_powerbi"
    description = """
    Input to this tool is a detailed and correct DAX query, output is a result from the database.
    If the query is not correct, an error message will be returned. 
    If an error is returned with Bad request in it, rewrite the query, check the query, and try again.
    If an error is returned with Unauthorized in it, do not try again, but tell the user to change their authentication.
    """

    def _run(self, tool_input: str) -> str:
        """Execute the query, return the results or an error message."""
        try:
            result = self.powerbi.run(command=tool_input)
        except Exception as exc:  # pylint: disable=broad-except
            if "bad request" in str(exc).lower():
                return "Bad request. Try rewriting the query and retrying then."
            if "unauthorized" in str(exc).lower():
                return "Unauthorized. Try changing your authentication, do not retry."
            return str(exc)
        return result

    async def _arun(self, tool_input: str) -> str:
        raise NotImplementedError("QueryPowerBITool does not support async")


class InfoPowerBITool(BasePowerBIDatabaseTool, BaseTool):
    """Tool for getting metadata about a PowerBI Dataset."""

    name = "schema_powerbi"
    description = """
    Input to this tool is a comma-separated list of tables, output is the schema and sample rows for those tables.
    Be sure that the tables actually exist by calling list_tables_powerbi first!
    
    Example Input: "table1, table2, table3"
    """

    def _run(self, tool_input: str) -> str:
        """Get the schema for tables in a comma-separated list."""
        return self.powerbi.get_table_info(tool_input.split(", "))

    async def _arun(self, tool_input: str) -> str:
        raise NotImplementedError("SchemaSqlDbTool does not support async")


class ListPowerBITool(BasePowerBIDatabaseTool, BaseTool):
    """Tool for getting tables names."""

    name = "list_tables_powerbi"
    description = "Input is an empty string, output is a comma separated list of tables in the database."

    def _run(self, tool_input: str = "") -> str:
        """Get the schema for a specific table."""
        return ", ".join(self.powerbi.get_table_names())

    async def _arun(self, tool_input: str = "") -> str:
        raise NotImplementedError("ListTablesSqlDbTool does not support async")


class QueryCheckerTool(BasePowerBIDatabaseTool, BaseTool):
    """Use an LLM to check if a query is correct.
    Adapted from https://www.patterns.app/blog/2023/01/18/crunchbot-sql-analyst-gpt/"""

    template: str = QUERY_CHECKER
    llm_chain: LLMChain = Field(
        default_factory=lambda: LLMChain(
            llm=OpenAI(temperature=0),
            prompt=PromptTemplate(template=QUERY_CHECKER, input_variables=["query"]),
        )
    )
    name = "query_checker_powerbi"
    description = """
    Use this tool to double check if your query is correct before executing it.
    Always use this tool before executing a query with query_powerbi!
    """

    @validator("llm_chain")
    def validate_llm_chain_input_variables(cls, llm_chain: LLMChain) -> LLMChain:
        """Make sure the LLM chain has the correct input variables."""
        if llm_chain.prompt.input_variables != ["query"]:
            raise ValueError(
                "LLM chain for QueryCheckerTool must have input variables ['query']"
            )
        return llm_chain

    def _run(self, tool_input: str) -> str:
        """Use the LLM to check the query."""
        return self.llm_chain.predict(query=tool_input)

    async def _arun(self, tool_input: str) -> str:
        return await self.llm_chain.apredict(query=tool_input)

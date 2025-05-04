# Build an AI agent using Python
from dotenv import load_dotenv
from pydantic import BaseModel
from langchain_openai import ChatOpenAI
from langchain_anthropic import ChatAnthropic
# import packages that will specify type of content we want our LLM to generate
from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain.agents import create_tool_calling_agent, AgentExecutor
# tools are things that the LLM/agent can use which we can write or can be brought in from Langchain community hub
from tools import search_tool, wiki_tool, save_tool

# Building an AI agent using Python and popular frameworks like LangChain
# Use of LLM Claude or GPT to give agent access to tools and structure output of agent to use in your code

load_dotenv()

# create a prompt template
# define python class which will specify type of content we want our LLM to generate
# The below class will be the output of the LLM call
# Can be more complex, so long as all classes inherit from BaseModel in pydantic
class ResearchResponse(BaseModel):
    topic: str
    summary: str
    sources: list[str]
    tools_used: list[str]

# define the language model to use - in this case claude, we could use GPT
# Generate the LLM
llm = ChatAnthropic(model="claude-3-5-sonnet-20240620")

# We could parse output as JSON, but here we choose Pydantic which is very popular in Python
# and is used by FastAPI and other frameworks
# PydanticOutputParser will take the output from the LLM and parse it into the specified class
# The output will be a JSON object that matches the ResearchResponse class
parser = PydanticOutputParser(pydantic_object=ResearchResponse)

# Create the prompt using the prompt template
prompt = ChatPromptTemplate.from_messages(
    [
        (
            "system",
            """
            "You are a helpful assistant that provides information about a request.
            Answer the user's question in a concise mannner. 
            Wrap the output in this format and provide no other text"\n{format_instructions}
            """
                    ),
        ("placeholder", "{chat_history}"),
        ("human", "{query}"),
        ("placeholder", "{agent_scratchpad}"),
    ]
).partial(format_instructions=parser.get_format_instructions())

# This uses our parser and takes the pydantic model and converts it to a string
# This string will be given to the prompt

tools = [search_tool, wiki_tool, save_tool]
agent = create_tool_calling_agent(
    llm=llm,
    prompt=prompt,
    tools=tools,
)

# We need the agentexecutor to execute the agent 
agent_executor = AgentExecutor(
    agent=agent,
    tools=tools,
    verbose=True
)

# Create input query which user can add themselves
query = input("What can I help you research?\n")

# {scratchpad} and {chat_history} are placeholders for the agent to fill in
raw_response = agent_executor.invoke({"query": query})
#print(raw_response)

try:
    # This will take the text from the first entry in the list and parse it using the parser into a python object called structured_response
    # The benefit of define a class for a reponse means you can take specific elements from the response as opposed to a large block of plain text
    structured_response = parser.parse(raw_response.get("output")[0]["text"])
    print("Structured response: ", structured_response)
except Exception as e:
    print("Error parsing response", e, "Raw response - ", raw_response)
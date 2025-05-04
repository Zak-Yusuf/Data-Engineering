# Bringing in tools from the web like wikipedia and duckduckgo
from langchain_community.tools import WikipediaQueryRun, DuckDuckGoSearchRun
from langchain_community.utilities import WikipediaAPIWrapper
from langchain.tools import Tool
from datetime import datetime

search = DuckDuckGoSearchRun()

# Tool No.1: DuckDuckGo
# seach tool is a wrapper for the DuckDuckGoSearchRun tool
search_tool = Tool(
    name="search",
    func=search.run,
    description="Search the web for information",
)

# Tool No.2 - Wikipedia
# wikipedia tool is a wrapper for the WikipediaAPIWrapper tool
api_wrapper = WikipediaAPIWrapper(top_k_results=1, doc_content_chars_max=100)

wiki_tool = WikipediaQueryRun(api_wrapper=api_wrapper)

# Tool No.3 - Custom tool which we write ourself and save to file
def save_to_txt(data: str, filename: str = "research_output.txt"):
    """
    Save the data to a text file with a timestamped filename.
    """
    # Get the current timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    formatted_text = f"--- Research Output ---\nTimestamp: {timestamp}\n\n{data}\n\n"

    # Write the data to the file
    with open(filename, "a", encoding="utf-8") as file:
        file.write(formatted_text)
    
    return f"Data successfully saved to {filename}"

# Once you define your own search function, you simply add it as a tool
save_tool = Tool(
    name="save_txt_to_file",
    func=save_to_txt,
    description="Save the research output to a text file with a timestamp.",
)
# Remember you can pass as many tools as you like to the agent executor in the main.py file
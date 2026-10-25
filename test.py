from langchain_openai import ChatOpenAI
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser

# Model
llm = ChatOpenAI(model="gpt-4o-mini")   # or any OpenAI model

# Prompt
template = PromptTemplate(
    input_variables=["topic"],
    template="Explain {topic} in simple terms."
)

# Chain = Prompt → LLM → OutputParser
chain = template | llm | StrOutputParser()

# Run
result = chain.invoke({"topic": "quantum computing"})
print(result)

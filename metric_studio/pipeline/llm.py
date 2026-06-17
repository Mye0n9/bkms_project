from langchain_anthropic import ChatAnthropic
from langchain_openai import ChatOpenAI
from config import settings


def get_llm():
    if settings.llm_provider == "openai":
        return ChatOpenAI(model=settings.llm_model, api_key=settings.openai_api_key)
    return ChatAnthropic(model=settings.llm_model, api_key=settings.anthropic_api_key)

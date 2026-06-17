from unittest.mock import MagicMock, patch
from pipeline.llm import get_llm
from config import settings


def test_get_llm_returns_chat_anthropic_by_default():
    with (
        patch.object(settings, "llm_provider", "anthropic"),
        patch("pipeline.llm.ChatAnthropic") as mock_anthropic,
    ):
        mock_anthropic.return_value = MagicMock()
        llm = get_llm()

    mock_anthropic.assert_called_once_with(model=settings.llm_model, api_key=settings.anthropic_api_key)
    assert llm is mock_anthropic.return_value


def test_get_llm_returns_chat_openai_when_provider_is_openai():
    with (
        patch.object(settings, "llm_provider", "openai"),
        patch("pipeline.llm.ChatOpenAI") as mock_openai,
    ):
        mock_openai.return_value = MagicMock()
        llm = get_llm()

    mock_openai.assert_called_once_with(model=settings.llm_model, api_key=settings.openai_api_key)
    assert llm is mock_openai.return_value

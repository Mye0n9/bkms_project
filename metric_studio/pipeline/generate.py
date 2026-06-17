from pathlib import Path
import yaml
from jinja2 import Environment, FileSystemLoader
from langchain_core.messages import HumanMessage
from pipeline.state import AgentState
from pipeline.llm import get_llm

PROMPTS_DIR = Path(__file__).parent.parent / "prompts"

SCHEMA = {
    "base": (
        "public.tickers(ticker_id INT PK, ticker VARCHAR, exchange_code VARCHAR, "
        "is_etf BOOL, lstg_yn BOOL)\n"
        "public.daily_prices(ticker_id INT FK, xymd DATE, clos NUMERIC, open NUMERIC, "
        "high NUMERIC, low NUMERIC, tvol BIGINT)"
    ),
    "daily_ma":           "public.daily_ma(ticker_id INT, xymd DATE, ma_5, ma_10, ma_20, ma_50, ma_200 NUMERIC)",
    "daily_bb":           "public.daily_bb(ticker_id INT, xymd DATE, ma_20, upper_band, lower_band, bandwidth, pct_b NUMERIC)",
    "daily_atr":          "public.daily_atr(ticker_id INT, xymd DATE, true_range, atr_14 NUMERIC)",
    "daily_obv":          "public.daily_obv(ticker_id INT, xymd DATE, obv BIGINT)",
    "calc_rsi":           "public.calc_rsi(p_window INT, p_as_of DATE) → (ticker_id, xymd, rsi_value, clos)",
    "calc_macd":          "public.calc_macd(p_fast INT, p_slow INT, p_signal INT, p_as_of DATE) → (ticker_id, xymd, macd_line, signal_line, histogram, clos)",
    "calc_momentum":      "public.calc_momentum(p_window INT, p_as_of DATE) → (ticker_id, xymd, momentum, clos)",
    "calc_rolling_return": "public.calc_rolling_return(p_window INT, p_as_of DATE) → (ticker_id, xymd, return_pct, clos)",
    "calc_volatility":    "public.calc_volatility(p_window INT, p_as_of DATE) → (ticker_id, xymd, volatility, clos)",
    "calc_pv_divergence": "public.calc_pv_divergence(p_window INT, p_as_of DATE) → (ticker_id, xymd, price_trend, vol_trend, divergence BOOL, clos)",
}


def _schema_context(metric_spec: dict) -> str:
    lines = [SCHEMA["base"]]
    key = metric_spec.get("function") or metric_spec.get("table", "")
    if key in SCHEMA:
        lines.append(SCHEMA[key])
    return "\n".join(lines)


def _full_schema_context() -> str:
    return "\n".join(SCHEMA.values())


def _load_few_shots(metric_id: str) -> list[dict]:
    path = PROMPTS_DIR / "few_shots" / "examples.yaml"
    all_examples = yaml.safe_load(path.read_text())
    return [ex for ex in all_examples if ex["metric_id"] == metric_id]


def _strip_markdown_fences(sql: str) -> str:
    if sql.startswith("```"):
        sql = "\n".join(sql.split("\n")[1:])
        sql = sql.rstrip("`").strip()
    return sql


def _invoke_llm(prompt_text: str, verbose: bool = False) -> str:
    llm = get_llm()
    response = llm.invoke([HumanMessage(content=prompt_text)])
    raw = response.content.strip()
    if verbose:
        print("\n[VERBOSE] GENERATE — raw LLM response:")
        print("-"*60)
        print(raw)
        print("="*60 + "\n")
    return _strip_markdown_fences(raw)


def generate(state: AgentState) -> AgentState:
    env = Environment(loader=FileSystemLoader(str(PROMPTS_DIR)))
    template = env.get_template("generate.jinja2")

    metric_spec = state["metric_spec"]
    prompt_text = template.render(
        metric_spec=metric_spec,
        schema=_schema_context(metric_spec),
        few_shots=_load_few_shots(metric_spec["id"]),
        error=state.get("execution_error", ""),
    )

    verbose = state.get("verbose", False)
    if verbose:
        print("\n" + "="*60)
        print("[VERBOSE] GENERATE — prompt sent to LLM:")
        print("-"*60)
        print(prompt_text)
        print("="*60)

    return {**state, "sql": _invoke_llm(prompt_text, verbose=verbose), "execution_error": None}


def generate_freeform(state: AgentState) -> AgentState:
    env = Environment(loader=FileSystemLoader(str(PROMPTS_DIR)))
    template = env.get_template("generate_freeform.jinja2")

    prompt_text = template.render(
        schema=_full_schema_context(),
        intent=state.get("intent", ""),
        raw_query=state.get("raw_query", ""),
        error=state.get("execution_error", ""),
    )

    verbose = state.get("verbose", False)
    if verbose:
        print("\n" + "="*60)
        print("[VERBOSE] GENERATE FREEFORM — prompt sent to LLM:")
        print("-"*60)
        print(prompt_text)
        print("="*60)

    return {**state, "sql": _invoke_llm(prompt_text, verbose=verbose), "execution_error": None}

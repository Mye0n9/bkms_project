# Term Project Proposal: Metric Studio

## Project Title

**Metric Studio: An NL2SQL Agent for Exploring Time-Series Dynamics in Securities Data**

## Team

- Rian Kim
- Myeongseop Kim
- Hyeonggeun Jeon

## 1. Problem Statement and Motivation

Individual investors who follow a value-investing strategy often use **relative valuation** methods, such as comparing PER and PBR across similar companies within the same industry. However, relative valuation has an inherent limitation: when the entire market or a specific sector becomes collectively overheated or depressed, the comparison baseline itself can become distorted.

For example, during a rapid market uptrend, an investor may mistakenly believe that a stock is undervalued simply because it appears cheaper than other even more overvalued securities. This can lead to the error of buying assets near historical highs.

To overcome this limitation, relative valuation should be complemented with an analysis of **time-series dynamics**, which reflects the movement of price and trading volume over time. Metrics that capture trend, momentum, volatility, and related dynamics can help detect temporary market distortions and correct errors in relative valuation.

In addition, exploring repeated patterns embedded in long-term time-series data can help individual investors avoid emotionally driven trading decisions. It can also support more objective investment decisions and risk management based on statistical advantages.

However, directly querying and analyzing large-scale time-series securities data requires substantial technical expertise. Moreover, many metrics used in dynamic analysis depend on user-defined parameters, which makes natural-language queries ambiguous. For instance, the interpretation of a metric-based market condition may vary depending on the user's assumptions, selected thresholds, rolling windows, and metric definitions.

This project proposes an **LLM-based NL2SQL agent** that translates natural-language screening conditions into SQL queries over securities time-series data stored in **PostgreSQL**. The key feature of the agent is that it actively asks **clarifying questions** whenever the user's query is ambiguous. By refining the user's analytical intent before generating SQL, the system aims to reduce ambiguity and support more advanced exploration of time-series dynamics.

## 2. Project Topic and Approach

### 2.1 Core Idea

The project builds an NL2SQL agent that allows users to explore time-series securities data through natural language. Instead of requiring users to manually write SQL queries or define complex metric calculations, the agent interprets the user's intent, detects ambiguity, asks clarifying questions, and generates SQL queries based on a refined metric specification.

### 2.2 Agent Workflow

1. **Receive a natural-language query**
   - The user enters a natural-language request for time-series dynamics analysis.
   - Example: “Find stocks that recently show overbought signals” or “Find securities with price-volume divergence.”

2. **Detect ambiguity in the query**
   - The agent identifies missing or ambiguous metric definitions, parameter values, time periods, and thresholds.
   - Examples of ambiguity:
     - Which metric pair defines “divergence”?
     - How many trading days does “recently” mean?
     - What threshold defines an “overbought” condition?
     - What rolling-window size should be used to calculate the metric?

3. **Ask clarifying questions before SQL generation**
   - Before generating or executing SQL, the agent asks the user clarifying questions to refine the analysis intent.
   - The goal is to convert an ambiguous natural-language request into a concrete metric specification.

4. **Generate schema-aware SQL**
   - After receiving the user’s feedback, the agent performs NL2SQL conversion based on the refined metric specification.
   - The SQL generation process should be aware of the PostgreSQL database schema.

5. **Execute the query and return results**
   - The generated SQL query is executed against the securities time-series database.
   - Query results are returned in relational/table form so that the user can inspect, validate, and provide feedback.

### 2.3 Use of Metric Studio Patterns

The project plans to use popular patterns introduced in Professor Moon Byung-ro’s book **Metric Studio** as a basis for metric-driven screening logic.

These patterns can be encoded as **parameterized SQL templates** and used in two ways:

- As few-shot examples for the NL2SQL agent.
- As a canonical answer space for resolving ambiguity in user queries.

This design helps the agent map vague user expressions to a structured set of known metric patterns and parameter choices.

### 2.4 Metric Computation Strategy

The project distinguishes between two types of metrics:

#### 2.4.1 General Parameter-Based Metrics

General metrics that can be reused frequently should be precomputed and stored in separate database tables.

Purpose:

- Improve query performance.
- Avoid recalculating common metrics repeatedly.
- Support fast screening over large-scale securities data.

#### 2.4.2 Variable Metrics Requiring User Parameters

Metrics that depend on user-specified parameters should be provided as predefined PostgreSQL functions.

Examples of variable parameters:

- Time period
- Rolling-window size
- Threshold value
- Metric-specific configuration

These parameters should be passed as binding variables when generating and executing SQL.

## 3. Database Design

### 3.1 Data Source

The primary data source is the **Korea Investment & Securities Open API**.

### 3.2 Data Coverage

The database will contain historical daily securities data for:

- Stocks listed on NYSE
- Stocks listed on NASDAQ
- ETFs listed on NYSE or NASDAQ

The main historical data range is:

- **10 years of daily OHLCV data**

OHLCV includes:

- Open price
- High price
- Low price
- Close price
- Trading volume

Additional data may also be collected if needed for the project.

### 3.3 Data Update Policy

The database should be updated every trading day after the regular market closes.

Each update should append the latest daily OHLCV data to the database.

## 4. Functional Requirements

### 4.1 Natural-Language Query Input

The system should accept natural-language queries related to securities time-series dynamics.

The user should not be required to manually write SQL.

### 4.2 Ambiguity Detection

The system should detect ambiguous expressions related to:

- Metric definitions
- Metric pairs
- Time ranges
- Rolling-window sizes
- Thresholds
- Screening conditions
- Terms such as “recently,” “overbought,” “divergence,” and similar domain-specific expressions

### 4.3 Clarifying Question Generation

When ambiguity is detected, the system should ask the user clarifying questions before SQL generation.

The clarifying questions should help convert the user’s request into a concrete metric specification.

### 4.4 Schema-Aware SQL Generation

The system should generate SQL queries that are compatible with the PostgreSQL schema.

The generated SQL should reflect:

- The refined user intent
- The selected metrics
- User-selected parameters
- Relevant tables or PostgreSQL functions

### 4.5 Query Execution and Result Presentation

The system should execute the generated SQL query and return the result as a relational table.

The result should be easy for the user to inspect and verify.

## 5. Non-Functional Goals

### 5.1 Interpretability

The system should make the query-generation process understandable by clearly identifying what assumptions, metrics, and parameters are used.

### 5.2 Performance

Frequently used metrics should be precomputed and stored to improve query speed.

Variable metrics should be implemented as PostgreSQL functions to support flexible parameter binding.

### 5.3 User Feedback Loop

The system should allow the user to review query results and provide feedback for further refinement.

## 6. Suggested Implementation Outline for a Code Assistant

A code assistant implementing this project should focus on the following components:

### 6.1 Database Layer

- Store 10 years of daily OHLCV data for NYSE and NASDAQ stocks and ETFs.
- Support daily appending of new OHLCV data after the regular market closes.
- Prepare separate storage for frequently used precomputed metrics.
- Implement PostgreSQL functions for metrics that require user-defined parameters.

### 6.2 Metric Specification Layer

- Define a structured metric specification format.
- The specification should include:
  - Metric name
  - Required input columns
  - Time range
  - Rolling-window size
  - Thresholds
  - Metric pair, if applicable
  - SQL template or PostgreSQL function mapping

### 6.3 Ambiguity Detection Layer

- Parse the user's natural-language query.
- Identify missing or vague parameters.
- Decide whether the query is specific enough for SQL generation.

### 6.4 Clarifying Question Layer

- Generate targeted clarifying questions for missing information.
- Use the user's answers to update the metric specification.

### 6.5 NL2SQL Layer

- Convert the finalized metric specification into schema-aware SQL.
- Use parameterized SQL templates whenever possible.
- Use binding variables for user-defined parameters.

### 6.6 Query Execution Layer

- Execute generated SQL queries on PostgreSQL.
- Return results in relational/table form.
- Preserve enough information for the user to verify the query result.

## 7. Example Ambiguity Cases

### Case 1: “Recently”

User query:

> Find stocks that recently showed strong momentum.

Possible ambiguity:

- Does “recently” mean 5 trading days, 20 trading days, or another period?

Clarifying question:

> How many trading days should be used to define “recently”?

### Case 2: “Divergence”

User query:

> Find stocks with price-volume divergence.

Possible ambiguity:

- Which two metrics define the divergence?
- Should divergence mean price increasing while volume decreases, or another relationship?

Clarifying question:

> Which metric pair should be used to define divergence, and what relationship between them should be treated as divergence?

### Case 3: “Overbought”

User query:

> Find overbought stocks.

Possible ambiguity:

- Which metric defines overbought status?
- What threshold should be used?
- What rolling-window size should be used?

Clarifying question:

> Which metric and threshold should be used to define the overbought condition, and what rolling-window size should be applied?

## 8. Project Scope

This project focuses on building an NL2SQL agent for securities time-series dynamics analysis.

The project scope includes:

- Natural-language query interpretation
- Ambiguity detection
- Clarifying question generation
- Metric specification refinement
- Schema-aware SQL generation
- PostgreSQL query execution
- Relational result presentation

The project does not require the user to manually write SQL queries.

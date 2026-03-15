
# 1. When would you not use an AI agent for a data task?

- **Sensitive or confidential data**  
  Avoid using AI agents on **PII, supplier/merchant data, or financial terms** (e.g., commission or premium rates) due to privacy and compliance risks.

- **Security-critical logic**  
  Do not use AI for **hashing, encryption, authentication, or secret handling**, which must remain deterministic and auditable.

- **Critical business definitions**  
  For important metrics (e.g., retention or revenue logic), AI can assist with documentation or analysis, but the **core transformation logic should remain deterministic and human-reviewed**.

---

# 2. How do you evaluate the quality of LLM-generated outputs in a data context?  
What does a good eval look like vs. a bad one?

## Evaluation Pillars

### 1. Grounding (Is the output based on the source data?)

- The LLM output should be evaluated against **source context**, such as SQL logic, schema metadata, or upstream documentation.

### 2. Deterministic validation (Rule-based checks)

Ensure structural and semantic requirements are met, including:

- description length within limits
- avoiding generic phrases
- referencing column transformation logic
- ensuring column names exist in the schema

### 3. Human feedback (Review signals)

Human reviewers provide important quality signals.

Metrics include:

- approval rate  
- edit distance between generated and approved descriptions  
- rejection reasons  

These signals help detect patterns where the LLM consistently misunderstands certain transformations.

### 4. Continuous monitoring (Production reliability)

Once deployed, monitor:

- documentation coverage  
- rejection rate trends  
- validation failures  
- benchmark performance over time  

Alerts can detect when **model updates or prompt changes cause quality degradation**.

---

## Good vs Bad Evaluation of LLM Outputs (Data Context)

| Dimension | Good Evaluation | Bad Evaluation |
|---|---|---|
| **Grounding** | Checks whether outputs are consistent with **source data, SQL logic, or schema metadata** | Evaluates outputs without comparing them to the underlying data logic |
| **Evaluation dataset** | Uses a **benchmark set of real production examples** with known correct outputs | Uses a few **handpicked or synthetic examples** |
| **Correctness criteria** | Measures **semantic correctness and factual accuracy** | Judges outputs mainly on **readability or fluency** |
| **Validation methods** | Combines **rule-based validation, benchmark tests, and human review signals** | Relies only on manual inspection or subjective judgment |
| **Scalability** | Evaluates outputs across **representative workloads at scale** | Tests only a small number of examples |
| **Monitoring** | Includes **continuous monitoring and regression testing** when models or prompts change | No monitoring after deployment |
| **Feedback loop** | Tracks **reviewer acceptance rates, edits, and rejection patterns** | No feedback loop from human reviewers |
| **Failure detection** | Designed to detect **hallucinations, generic outputs, and semantic errors** | Only detects obvious grammatical issues |

---

# 3. If the documentation agent shipped and started producing subtly wrong descriptions at scale, how would you catch it before it caused harm downstream?

- **Human review before merge**  
  The agent only proposes changes in a PR. It generates a report showing **what changed, which columns were updated, and any low-confidence outputs** for reviewers to inspect.

- **Monitor review signals**  
  Track **rejection rate, edit frequency, and documentation coverage** across Data Engineers and Analysts.  
  A spike in edits or rejections signals quality issues.

- **Automated alerts**  
  Trigger alerts if **approval rates drop or low-confidence outputs increase**, indicating the agent may be drifting or producing incorrect descriptions at scale.

---

# 4. What is one AI-native capability you wish existed in the modern data stack today?

One AI-native capability I wish existed is a **semantic impact analyzer for metrics**.

Today, most data tools can show **table lineage**, but they cannot understand whether a change alters the **business meaning of a metric**.

For example, in a rewards platform like **HeyMax**, we might define **retained users** as users who made a transaction in the last **30 days**:

```sql
where event_date >= current_date - 30
```

If a developer later changes the logic to **45 days**:

```sql
where event_date >= current_date - 45
```

The table structure stays the same, so existing tools only show that downstream dashboards depend on the model.  
They **cannot detect that the definition of “retained users” has changed**.

An AI-native system could analyze **SQL logic, metric definitions, and downstream queries**, then warn:

> “The retained users window changed from 30 → 45 days.  
> This may impact CRM segmentation, retention dashboards, and marketing experiments.”

In a platform like **HeyMax**, this could prevent unintended impacts on **at-risk user targeting, retention campaigns, and executive reporting**, acting as a **semantic guardrail in the development workflow**.

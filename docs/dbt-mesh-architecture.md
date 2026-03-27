# ADR-001: Adopting dbt Mesh for Supply Chain Data Platform

---

## Context

The grocery retailer operates a monolithic dbt project with 200+
models maintained by a single centralised data team. As the business
has grown to 400 stores across five states, this architecture has
produced three compounding problems:

**Problem 1 — Deployment bottleneck**  
Every model change — regardless of which business domain it affects
— requires a full project deployment. A fix to a merchandising metric
blocks a supply chain model change from going live. Deployment queues
regularly stretch to 3–4 days.

**Problem 2 — Unclear ownership**  
When `fct_sales` breaks, it is unclear whether the supply team
(who own inventory data) or the demand team (who own POS data) is
responsible for the fix. Incidents take 2–3x longer to resolve than
they should.

**Problem 3 — Monolith fragility**  
A breaking change in a raw source table cascades through all 200+
models simultaneously. There is no isolation between domains —
a supplier EDI schema change can break the revenue dashboard.

---

## Decision

We will adopt the **dbt Mesh** pattern: decomposing the monolithic
dbt project into two domain-aligned projects — `supply` and `demand`
— with explicit, versioned cross-domain contracts.

### Domain boundaries

| Domain | Owns | Public models |
|---|---|---|
| `supply` | Stores, products, inventory, purchase orders, suppliers | `dim_stores`, `dim_products`, `fct_inventory_snapshots` |
| `demand` | POS sales, stockout analysis, demand forecasting | `fct_sales`, `fct_stockouts` |

The boundary follows the natural split in the business:
- **Supply** = what we have and what we've ordered
- **Demand** = what customers are buying and what they want

### Cross-domain contract model

The demand domain references supply's public models using dbt's
cross-project ref syntax:
```sql
{{ ref('supply', 'dim_products') }}
```

Public models are declared stable contracts. Changes to their
column names, data types, or grain require a versioning process
(see Governance below). Breaking changes are not permitted without
a deprecation period.

### What "public" means

A model marked as public in dbt Mesh means:
- Other domains may reference it
- The owning team is responsible for its stability
- Schema changes follow the versioning process below
- The model has documented grain, column definitions, and tests

Models not marked public are internal to the domain and may
change freely.

---

## Governance model

### Domain ownership

Each domain is owned by a named team with a named lead:

| Domain | Team | Lead | Slack channel |
|---|---|---|---|
| supply | Supply Chain Engineering | TBD | #data-supply |
| demand | Demand Analytics | TBD | #data-demand |

Domain leads are responsible for:
- Reviewing and merging PRs to their domain
- Maintaining public model contracts
- Responding to incidents in their domain within SLA
- Quarterly review of Elementary anomaly alerts

### Contract versioning

Public model changes follow semantic versioning:

**Patch (no version bump required)**
- Adding a new column
- Improving a column description
- Adding a new test

**Minor (deprecation notice required)**
- Renaming a column (add new name, deprecate old name,
  remove after 30 days)
- Changing a column's data type
- Adding a NOT NULL constraint to an existing column

**Major (cross-domain migration required)**
- Changing the grain of a model
- Removing a column
- Renaming the model itself

Major changes require:
1. 30-day advance notice in #data-platform
2. Joint migration plan with all consuming domains
3. Sign-off from consuming domain leads

### PR process

All model changes go through GitHub pull requests with:
- Automated `dbt test` run via GitHub Actions
- Mandatory review from domain lead
- Elementary anomaly check on affected models
- Updated `marts.yml` documentation

---

## When to split a monolith

This ADR establishes the principle for future domain splits.
The criteria for creating a new domain are:

**Split when:**
- A group of models has a clearly distinct business owner
  who makes independent deployment decisions
- A domain's deployment cycle is blocked by unrelated changes
- A domain has materially different SLAs (e.g. finance models
  need daily refresh; marketing models are weekly)
- A team has grown large enough to own its own CI/CD pipeline

**Do not split when:**
- The motivation is purely technical preference
- The domain would have fewer than 10 models
- No clear human owner exists for the new domain
- The cross-domain contract would require daily schema changes

The cost of a split is real: cross-domain debugging is harder,
onboarding new engineers takes longer, and contract versioning
adds process overhead. Split only when the coordination cost
of staying together exceeds the coordination cost of separating.

---

## Alternatives considered

### Alternative 1: Remain monolithic, add team namespaces
Organise models into folders by team but keep a single project.
**Rejected:** Does not solve deployment bottleneck or ownership
ambiguity. Folder conventions erode over time without enforcement.

### Alternative 2: Separate data warehouses per domain
Each domain writes to its own DuckDB/Snowflake instance.
**Rejected:** Cross-domain joins become expensive API calls.
Referential integrity cannot be enforced across warehouses.
Operational overhead is significant.

### Alternative 3: Three-domain split (supply, demand, finance)
Add a finance domain immediately.
**Deferred:** Finance reporting currently has 8 models maintained
by one analyst. Does not meet the minimum viable domain threshold.
Revisit in Q3 2024 when finance team expands.

---

## Consequences

**Positive:**
- Supply and demand teams deploy independently
- Breaking changes are isolated to their domain
- Public contracts create a forcing function for documentation
- Elementary can be configured per domain with domain-specific
  anomaly thresholds

**Negative:**
- Cross-domain debugging requires understanding two codebases
- New engineers must learn the Mesh pattern before being productive
- The `supply/target/manifest.json` dependency creates a coupling
  in CI/CD — supply must be compiled before demand can run

**Risks:**
- Contract drift: public models change without following the
  versioning process. Mitigated by automated contract tests in CI.
- Over-splitting: teams are tempted to create new domains for
  every new dataset. Mitigated by the split criteria above.

---
# CBR Confidence Model

This document is the canonical specification for the Confidence-Based Review (CBR) algorithm.
Any AI agent or human operator running a CBR loop must reconstruct belief scores from this
document alone — no prior conversation history is required.

---

## Purpose

CBR answers: *how confident are we that kotlin-otp faithfully represents Erlang/OTP design
intent, and how do we converge that confidence to 99%?*

It tracks two orthogonal concerns:

- **Correctness axis** — does the implementation behave correctly (safety, liveness, no hazards)?
- **Fidelity axis** — does the implementation faithfully represent OTP design intent (parity,
  intentional divergence, or registered exclusion)?

These are tracked as **Claims** in `cbr-claim-registry.md`. Evidence is gathered per claim,
belief scores are updated by defined rules, and a portfolio confidence is computed across all claims.

---

## Claim Structure

```
Claim {
  id:                string       // e.g. "genserver+supervisor.restart_deferred_reply"
  components:        string[]     // e.g. ["genserver", "supervisor"]
  interaction_depth: int          // 1 = isolated, 2 = pairwise, 3 = three-way
  axis:              Correctness | Fidelity
  sub_axis:          Safety | Liveness | Parity | IntentionalDivergence | Exclusion
  statement:         string       // falsifiable assertion in plain language
  status:            Unverified | EvidenceGathering | Confirmed | Inconclusive |
                     Refuted | RemediationOpen | RemediationComplete |
                     Reclassified | Verified | Exempted
  belief:            float [0.0, 1.0]
  threshold:         float        // belief required to close (default 0.95)
  evidence:          Evidence[]
}
```

### ID Convention

- Depth-1: `<component>.<property>` — e.g. `genserver.sequential_callback`
- Depth-2: `<comp1>+<comp2>.<property>` — e.g. `genserver+supervisor.restart_deferred_reply`
- Depth-3: `<comp1>+<comp2>+<comp3>.<property>`

Components are always listed in alphabetical order to avoid duplicate IDs for the same pair.

---

## Evidence Taxonomy

Each piece of evidence has a **type** (base weight) and a **quality** multiplier.

### Evidence Types

| Type | Base weight | Rationale |
|---|---|---|
| `AdversarialTest` | 0.30 | Directly stresses the claim under hostile conditions |
| `DifferentialTest` | 0.25 | Side-by-side comparison against Erlang/OTP reference behaviour |
| `StructuralInspection` | 0.15 | Code-level review of the implementation path |
| `InvariantSpec` | 0.10 | Formal statement of the property being claimed |
| `BenchmarkObservation` | 0.10 | Quantitative measurement supporting the claim |
| `NarrativeRationale` | 0.05 | Prose justification; lowest weight, easy to produce |
| `ExpertReview` | 0.05 | Manual reasoning; included but discounted |

### Quality Multipliers

| Quality | Multiplier |
|---|---|
| `High` | 1.0 |
| `Medium` | 0.7 |
| `Low` | 0.4 |

---

## Belief Update Rules

Evidence gathering produces one of three **outcomes**:

| Outcome | Condition | Belief delta |
|---|---|---|
| `Confirmed` | Evidence supports the claim | `+(base_weight / n) × quality` where `n` = count of same type already present |
| `Inconclusive` | Evidence gathered but neither confirms nor falsifies | `+(base_weight / n × quality) × 0.3` |
| `Refuted` | Evidence falsifies the claim | `belief × 0.5` (penalty multiplier — not additive) |

### Accumulation formula (Confirmed evidence only)

```
belief(C) = min(threshold(C),
               Σ for each e in confirmed_evidence(C):
                 (base_weight(e.type) / rank(e)) × quality(e))
```

where `rank(e)` is the 1-based position of evidence `e` among all confirmed evidence of the
same type for claim `C`, in arrival order.

This enforces **diminishing returns**: the second adversarial test is worth half the first;
the third is worth a third; and so on. This prevents artificially inflating belief by
running many tests of the same kind.

### Refutation

Refutation overrides the accumulation sum:

```
belief(C) = current_belief(C) × 0.5
status(C) = Refuted
```

A reset to 0.0 would be too harsh — prior structural evidence (inspections, invariant specs)
was genuine. Halving correctly signals degraded confidence while preserving partial credit.

### Inconclusive evidence

Inconclusive evidence produces a small rise (30% of the normal delta). It records that effort
was spent and something was learned, without overstating what was found.

---

## Claim State Machine

```
Unverified
  → EvidenceGathering       (claim selected by loop)

EvidenceGathering
  → Confirmed               (outcome is Confirmed; belief rises)
  → Inconclusive            (outcome is Inconclusive; belief rises slightly)
  → Refuted                 (outcome is Refuted; belief halved)

Confirmed
  → Verified                (belief >= threshold)
  → EvidenceGathering       (belief < threshold; more evidence needed)

Inconclusive
  → EvidenceGathering       (retry with higher-weight evidence type)

Refuted
  → RemediationOpen         (operator enters fix mode)
  → Reclassified            (deviation judged intentional; new Divergence claim opened)

RemediationOpen
  → RemediationComplete     (fix applied to implementation)

RemediationComplete
  → EvidenceGathering       (evidence list cleared; belief reset to seed; restart)

Reclassified
  → Verified                (original claim closed at belief 0.0;
                             new IntentionalDivergence claim opened at seed belief)

Unverified
  → Exempted                (irreproducible; rationale documented)

Verified  → terminal
Exempted  → terminal
```

### State rules

- **`RemediationOpen`**: at most one claim may be in this state at a time.
  Portfolio confidence is **frozen** while any claim is in `RemediationOpen` or
  `RemediationComplete` — fixes that have not been retested do not count.

- **`RemediationComplete → EvidenceGathering`**: the claim's `evidence` list is **cleared**
  and `belief` is reset to its seed value. Prior evidence was gathered against the broken
  implementation and cannot carry over.

- **`Reclassified`**: the original claim closes at `belief = 0.0`. A new claim with
  `sub_axis = IntentionalDivergence` is opened at a seed belief reflecting documentation
  already present. Net portfolio effect: small immediate drop, gradual recovery as the new
  claim accrues evidence.

- **`Exempted`**: contributes to portfolio confidence at `belief × 0.5` (partial credit for
  documented non-applicability; not full credit for verified behaviour).

---

## Portfolio Confidence

```
portfolio_confidence =
  Σ(effective_belief(C) × claim_weight(C)) / Σ(claim_weight(C))

effective_belief(C) =
  | belief(C)         if status ∈ {Verified, EvidenceGathering, Confirmed, Inconclusive}
  | belief(C) × 0.5   if status = Exempted
  | belief(C)         if status = Refuted  (penalty already applied to belief)
  | 0.0               if status = Unverified
```

### Claim weights

| Axis | Sub-axis | Depth | Weight |
|---|---|---|---|
| Correctness | Safety | any | 3 |
| Correctness | Liveness | any | 2 |
| Fidelity | Parity | 1 | 2 |
| Fidelity | Parity | 2 | 3 |
| Fidelity | Parity | 3 | 4 |
| Fidelity | IntentionalDivergence | any | 1 |
| Fidelity | Exclusion | any | 1 |

Depth-2 and depth-3 parity claims carry higher weight because cross-cutting fidelity is
harder to verify and more representative of the overall "meat on skeleton" concern.

---

## Hard Gate

**Portfolio confidence cannot be declared ≥ 99% unless:**

> Every adjacent component pair that exists in the codebase has at least one depth-2+ claim
> with status `Verified` or `Exempted`.

This prevents a high arithmetic average from masking a completely uninvestigated interaction
boundary. The hard gate must be checked *before* reporting convergence, even if the
portfolio formula returns ≥ 0.99.

### Current component pairs (codebase as of this writing)

| Pair | Required depth-2+ claim |
|---|---|
| genserver + mailbox | at least one |
| genserver + supervisor | at least one |
| genserver + distribution | at least one |
| supervisor + distribution | at least one |
| global + distribution | at least one |
| mailbox + supervisor | at least one (or Exempted with rationale) |

---

## Stopping Criterion

The loop terminates (portfolio is declared converged) when **all** of the following hold:

1. Every claim is in status `Verified`, `Exempted`, or `Reclassified`.
2. The hard gate is satisfied (all adjacent pairs have a depth-2+ closed claim).
3. `portfolio_confidence ≥ 0.99`.
4. No claim is in `RemediationOpen` or `RemediationComplete`.

If criterion 3 is met arithmetically but criteria 1 or 2 are not, open new claims from the
Unknown backlog or seed the missing interaction-depth claims before declaring convergence.

---

## Evidence Record Format

Each entry in a claim's `evidence_summary` column in `cbr-claim-registry.md` should record:

```
<type>/<quality>: <one-line description> [<file or test reference>] → <outcome>
```

Examples:
```
AdversarialTest/High: crash-during-deferred-reply stress [GenServerRobustnessContractTest.kt:L142] → Confirmed
StructuralInspection/Medium: reviewed ReplyHandle lifecycle in GenServer.kt → Confirmed
DifferentialTest/High: counter protocol vs real Erlang node [BehavioralEquivalenceTest.kt] → Inconclusive
NarrativeRationale/Low: documented in LIMITATIONS.md "gen_server gaps" section → Confirmed
```

---

## Relationship to Prior Artefacts

The CBR algorithm does not replace the correctness investigation artefacts. They become
**evidence sources** referenced from `cbr-claim-registry.md`:

| Artefact | Role in CBR |
|---|---|
| `correctness-spec.md` | Invariant specs → `InvariantSpec` evidence entries |
| `correctness-verdicts.md` | Verdict reasoning → `NarrativeRationale` or `ExpertReview` entries |
| `hazard-matrix.md` | Hazard test rows → `AdversarialTest` evidence entries |
| `parity-matrix.md` → `cbr-parity-matrix.md` | Each row links to a registry claim via `claim_id` |
| `otp-robustness-gap-catalog.md` | Gap rows inform seed belief and initial status of claims |
| `otp-modernization-exclusions.md` | Exclusion rows become `Fidelity/Exclusion` claims |
| `ergonomics-audit.md` | API quality observations → `NarrativeRationale` entries |
| `benchmark-algorithm-analysis.md` | Performance data → `BenchmarkObservation` entries |

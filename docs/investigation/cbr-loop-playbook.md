# CBR Loop Playbook

Per-iteration procedure for the Confidence-Based Review (CBR) loop.

Refer to `cbr-confidence-model.md` for the algorithm specification and scoring rules.
Refer to `cbr-claim-registry.md` for the current claim table and portfolio status.

---

## Operating Modes

The loop operates in exactly one mode at any time. The current mode must be recorded in
`cbr-claim-registry.md` under the `Portfolio Confidence` block.

### Mode 1: Gather (default)

Normal evidence-accumulation mode. Used when no claim is in `Refuted`, `RemediationOpen`,
or `RemediationComplete`.

**Entry**: start of loop, or after any Remediation or Reclassification mode exits.

**Exit**: a claim outcome is `Refuted` → enter Remediation mode.
         or: a claim outcome triggers reclassification → enter Reclassification mode.

### Mode 2: Remediation

Fix mode. Entered when evidence falsifies a claim.

**Entry**: claim status transitions to `Refuted`.

**Rules**:
- At most one claim may be in `RemediationOpen` at a time.
- Portfolio confidence display is **frozen** — do not update `portfolio_confidence` in
  `cbr-state-ledger.md` while in this mode.
- No other claims advance while Remediation is active.

**Exit sequence**:
1. Apply fix to implementation.
2. Set claim status to `RemediationComplete`.
3. Clear claim's `evidence_summary` in `cbr-claim-registry.md`.
4. Reset claim's `belief` to its seed value (see plan seed claims).
5. Set claim status to `EvidenceGathering`.
6. Return to Gather mode.

### Mode 3: Reclassification

Used when a `Refuted` claim is judged to reflect intentional design, not a bug.

**Entry**: operator decides that the violation is intentional divergence from OTP, not a defect.

**Exit sequence**:
1. Close the original claim: set `status = Reclassified`, `belief = 0.0`.
2. Open a new claim with `sub_axis = IntentionalDivergence`, seeded at an appropriate
   initial belief reflecting documentation already present.
3. Add a `NarrativeRationale` evidence entry to the new claim referencing the decision.
4. Return to Gather mode immediately.

---

## Per-Iteration Steps (Gather Mode)

### Step 1 — Check mode

Confirm no claim is in `RemediationOpen` or `RemediationComplete`. If so, resolve it first
(Remediation mode above).

### Step 2 — Check hard gate

Count how many adjacent component pairs in the hard gate table (`cbr-claim-registry.md`)
still have no `Verified` or `Exempted` depth-2+ claim.

- If **any pair is uncovered**: depth-2+ claims for uncovered pairs take **scheduling priority**
  regardless of their belief scores.
- If **all pairs covered**: select by lowest belief score (standard order).

### Step 3 — Select claim

Pick the claim with the lowest `belief` among open claims (status not in
`{Verified, Exempted, Reclassified}`), subject to the depth-2+ priority rule above.

Record the selected `claim_id` before proceeding.

### Step 4 — Identify next evidence type

Review the claim's current `evidence_summary`. Apply marginal value ordering:

1. Choose the highest-weight type not yet present (or with fewest entries if all types present).
2. Check feasibility: can this evidence type actually be gathered for this claim?
   - `AdversarialTest` / `DifferentialTest`: requires a runnable test or Erlang reference.
   - `StructuralInspection`: requires reading the relevant implementation files.
   - `InvariantSpec`: requires writing a formal statement of the property.
   - `BenchmarkObservation`: requires a measurable scenario.
   - `NarrativeRationale` / `ExpertReview`: always feasible.
3. If the highest-value type is not feasible, descend to the next type.
4. If no evidence is feasible for this claim, mark it `Exempted` with a documented rationale
   and proceed to the next claim.

### Step 5 — Gather evidence

Execute the chosen evidence type:

- **AdversarialTest**: write or run the adversarial test. Record result.
- **DifferentialTest**: compare against Erlang/OTP reference. Record result.
- **StructuralInspection**: read and analyse the implementation. Record findings.
- **InvariantSpec**: write the invariant statement. Record it.
- **BenchmarkObservation**: run the benchmark. Record measurements.
- **NarrativeRationale**: write the justification. Record it.
- **ExpertReview**: document the manual reasoning. Record it.

### Step 6 — Classify outcome

Assign one of three outcomes:

| Outcome | When to assign |
|---|---|
| `Confirmed` | Evidence clearly supports the claim statement |
| `Inconclusive` | Evidence gathered but result is ambiguous or partial |
| `Refuted` | Evidence directly falsifies the claim statement |

### Step 7 — Update belief

Apply the belief delta per `cbr-confidence-model.md`:

- `Confirmed`: `belief += (base_weight / rank) × quality`; cap at `threshold`.
- `Inconclusive`: `belief += (base_weight / rank × quality) × 0.3`.
- `Refuted`: `belief = belief × 0.5`; set `status = Refuted`; enter Remediation mode.

Record the new `belief` value and updated `evidence_summary` entry in `cbr-claim-registry.md`.

Format for `evidence_summary` entry:
```
<type>/<quality>: <one-line description> [<file or test ref>] → <outcome>
```

### Step 8 — Check claim closure

If `belief >= threshold`, set `status = Verified`.

If the claim was `Exempted` in step 4, it is already closed.

### Step 9 — Check hard gate + portfolio

After updating the claim:

1. Re-evaluate hard gate: update `cbr-claim-registry.md` Hard Gate Status table.
2. If all claims are `Verified`, `Exempted`, or `Reclassified` **and** hard gate is satisfied:
   - Compute `portfolio_confidence` using the formula in `cbr-confidence-model.md`.
   - Update the `Portfolio Confidence` block in `cbr-claim-registry.md`.
   - Update `portfolio_confidence` in `cbr-state-ledger.md`.
3. If `portfolio_confidence >= 0.99` and hard gate satisfied: **loop terminates**.
4. If `portfolio_confidence < 0.99`: open new claims from the Unknown Backlog and continue.

### Step 10 — Update ledger

After every iteration, update `cbr-state-ledger.md`:
- Increment the appropriate cost unit (`g`).
- Update the `portfolio_confidence` line if not frozen.
- Append a one-line summary of the iteration to the recomputed queue section.

---

## Deliverables Per Iteration

- Updated `evidence_summary` for the selected claim in `cbr-claim-registry.md`.
- Updated `belief` and `status` for the selected claim.
- Updated hard gate table.
- Updated `portfolio_confidence` block (if not frozen).
- One-line ledger entry in `cbr-state-ledger.md`.
- Any new test file or specification document referenced as evidence.

---

## Definition of Done (Loop)

1. Every claim in `cbr-claim-registry.md` is `Verified`, `Exempted`, or `Reclassified`.
2. Hard gate satisfied: all adjacent component pairs have at least one depth-2+ closed claim.
3. `portfolio_confidence >= 0.99`.
4. No claim is in `RemediationOpen` or `RemediationComplete`.
5. `cbr-state-ledger.md` reflects the final `portfolio_confidence`.

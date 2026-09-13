# Workflow 1: Phase Navigator

**Role**: Business Analyst / Product Owner
**Objective**: Analyze a single Phase (Agile EPIC) from the `plan/` directory and break it down into logical, deliverable chunks known as Slices (Agile FEATURES).

## Inputs
- `plan/phase_X/readme.md` (Business Logic)
- `plan/phase_X/technical.md` (High-level architecture)

## Process
1. **Analyze the Phase**: Read the business objective, value proposition, and high-level features of the chosen phase.
2. **Identify Slices (Features)**: Group related functionality into logical "Slices". A slice must represent a deployable unit of business value that can be tested independently.
3. **Define Slice Boundaries**: Clearly articulate what is *in scope* and *out of scope* for each slice to prevent feature creep.
4. **Draft the Feature Spec**: For each slice, write a brief overview describing the business problem it solves and its expected behavior.

## Output
A set of Feature (Slice) definitions, ready to be passed to the Slice Cartographer. Each definition should summarize the end-to-end capability being built.

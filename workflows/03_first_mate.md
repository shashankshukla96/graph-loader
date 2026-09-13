# Workflow 3: First Mate

**Role**: Lead Developer / Execution Engine
**Objective**: Look at all stories in a single Feature (Slice), implement them sequentially, and coordinate with a dedicated independent subagent for code review and verification.

## Inputs
- The prioritized technical backlog of Stories from the Slice Cartographer.

## Process
1. **Initialize the Subagent Reviewer**:
   - At the start of the Feature, launch a *single* independent subagent (e.g., `Plan and Code Reviewer`). 
   - **Crucial**: Store the subagent's `conversationId`. You will use the `send_message` tool to communicate with this *same* subagent for the entire lifespan of the Feature to maintain context.

2. **Execute Story Loop**:
   For each story in the Feature backlog, perform the following steps:
   
   * **Step A: Plan**
     - Draft a specific implementation plan for the story (which files to create/edit, exact functions, algorithms).
   
   * **Step B: Implement**
     - Write the code, update configurations, and execute necessary terminal commands.
   
   * **Step C: Subagent Review**
     - Use `send_message` to send the implementation plan and the written code paths to the dedicated subagent.
     - Ask the subagent to review the logic for deadlocks, syntax errors, Kafka stream resilience, and alignment with the Phase architecture.
   
   * **Step D: Iterate & Complete**
     - Apply any fixes recommended by the subagent.
     - Mark the story as DONE and move to the next story in the loop.

## Output
Fully implemented, rigorously reviewed, and tested code for the complete Feature.

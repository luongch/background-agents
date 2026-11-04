// import { setRun } from './agent-data'
import { randomUUID } from 'node:crypto'
import {
  agentState,
  getRun,
  goalSatisfied,
  recordDecision,
  recordToolAction,
  setIteration,
  setRun
} from './agent-data'
import { chooseAction, writeReport } from './agent-brain'
import { checkoutServiceUrl } from './observe'
import { inngest } from './inngest'

export const incidentAgent = inngest.createFunction(
  // id of function, human readable name for this function, what trigger that you would fire that would cause this function to evoke
  { id: 'incident-agent', name: 'Checkout incident agent', triggers: { event: 'incident/opened' } },
  async ({ event, step }) => { //think of a step as a checkpoint, once it gets run it wont get run again
    //get all the identifiers
    const { environmentId, runId } = event.data
    //one opaque checkpoint proves the loop, but hides where each effect happened.
    //we're going to put the entire workflow (which is very inefficient) into one step (for now)
    //doing this defeats the purpose of having inngest but I think it's for learning purposes
    return step.run('whole-agent-loop', async () => {
      //check if there's already an agent run that is happening by checking in the db
      //if there is already a run happening I can pick up where it left off
      const run = await getRun(runId)
      for (let cycle = 1; cycle <= 8; cycle++) {
        //at the top of the loop we want to do the first step which is 'observe the world'
        const state = await agentState(environmentId, runId)
        //after we get the state we should check if it has already reached the goal state, do we even need to do anything?
        //if the goal is already done then we can just write the report
        if(goalSatisfied(state, run.goalCondition)) {
          const report = await writeReport(run.goal, state)
          await setRun(runId, 'completed', null, report)
          return { report }
        }

        //now we need to choose the next thing to do
        const decision = await chooseAction(run.goal, state) //what should we do next given our overall goal and our current view of the world + whatever system prompt you have + the actions you have available to you and why should we take that action

        //the next 2 lines of code are basically logging, because when we have something that runs in the background
        //it's a good idea to store actions when it happens so we can explore how we got there
        //in a chat app you don't need this because you can just scroll up and read the chat and see how you got here
        //update iteration count
        await setIteration(runId, cycle)
        //record the decision because the AI responded so now we need to record it in the DB
        await recordDecision(environmentId, runId, cycle, decision.action, decision.reason)

        //was one of the decisions a special one?
        if(
          decision.action === 'wait' ||
          decision.action === 'complete' || //this one is if it got past goalSatisfied but then in the decision it decided it was complete, how do we take what it decided and put it back into goal statisfied
          decision.action === 'request_help'
        ) {
          await setRun(runId, 'escalated', 'This first loop cannot pause yet')
          return
        }

        //other actions we don't want the agent doing
        if(decision.action === 'rollback_release') {
          await setRun(runId, 'escalated', 'Approval gate is not built yet')
          return
        }

        //now that we have caught everything above we can now allow the agent to do it's thing 
        const actionId = `${runId}:${randomUUID()}`
        //when we make this api call, the action will be performed now
        //we assume that every action changes the state, if you use a state approach always keep that in mind, every action you take should modify the state so the agent can observe some change
        const response = await fetch(checkoutServiceUrl + '/operations', { //allows us to interface with that service state (built by the course instructor)
          method: 'POST',
          headers: { 'Content-Type': 'application/json'},
          body: JSON.stringify({
            actionId,
            name: decision.action,
            expectedVersion: state.world.version
          })
        })

        const result = (await response.json()) as Record<string, unknown>
        if(!response.ok) {
          throw new Error(String(result.error ?? response.status))
        }
        if(result.stale !== true) {
          await recordToolAction(
            environmentId,
            runId,
            actionId,
            decision.action,
            { expectedVersion: state.world.version},
            result
          )
        }               
      }

      await setRun(runId, 'escalated', 'Decision limit reached')
    })


    // await setRun(event.data.runId, 'escalated', 'Harness not implemented yet')
    // return { runId: event.data.runId, next: 'Build the observe–decide–act loop' }
  },
)

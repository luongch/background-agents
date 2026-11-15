import { randomUUID } from 'node:crypto'
import {
  agentState,
  getRun,
  goalSatisfied,
  recordDecision,
  recordToolAction,
  setIteration,
  setRun,
} from './agent-data'
import { chooseAction, writeReport } from './agent-brain'
import { inngest } from './inngest'
import { checkoutServiceUrl } from './observe'
import { logAgentActivity } from './agent-log'
import { actionPolicy } from './tool-policy'
import type { ActionName } from '../shared/types'


//helper function do what we were doing in lesson 1 which is take an action and make an api call to a service so the action can be called
//  but also we want it to log things before we call it, so we can track all the things it does
//encapsulates all the logic for executing an agent action: logging the activity, making an API call to the service to perform the action, handling the response,
//recording successful tool actions and logging failures. This prevent code duplication across multiple workflow steps that need to execute actions
async function executeAction(
  environmentId: string,
  runId: string,
  actionId: string,
  name: ActionName,
  expectedVersion?: number,
) {
  const input = expectedVersion === undefined ? {} : { expectedVersion }

  //anytime an agent does something we want to go ahead and log that activity
  await logAgentActivity(environmentId, runId, 'act', `Calling ${name.replaceAll('_', ' ')}`, {
    actionId, 
    input,
  })

  try {
    const response = await fetch(`${checkoutServiceUrl}/operations`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json'},
      body: JSON.stringify({actionId, name, ...input})
    })

    const result = (await response.json()) as Record<string, unknown>
    if (!response.ok) {
      throw new Error(String(result.error || `Tool failed: ${response.status}`))
    }

    if(result.stale !== true) {
      await recordToolAction(environmentId, runId, actionId, name, input, result)
    }

    return result
  } catch (error) {
    await logAgentActivity(environmentId, runId, 'act', `${name.replaceAll('_', ' ')} attempt failed`, {
      actionId,
      error: error instanceof Error ? error.message : String(error)
    })

    throw error
  }
}

export const incidentAgent = inngest.createFunction(
  { id: 'incident-agent',
    name: 'Checkout incident agent',
    triggers: { event: 'incident/opened' },
    retries: 2,
    concurrency: {limit: 1, key: 'event.data.environmentalId'},
    onFailure: async ({ error, event}) => {
      const original = event.data.event as { data?: { runId?: string}}
      if(original.data?.runId) {
        await setRun(original.data.runId, 'failed', error.message)
      }
    }
  },
  async ({ event, step }) => {
    const { environmentId, runId, instanceId } = event.data
    // One opaque checkpoint proves the loop, but hides where each effect happened.

    const run = await getRun(runId)
    //we need to see if this is still a valid run, did it already complete? did it fail? etc etc
    if(run.instanceId !== instanceId || ['completed', 'failed','cancelled','escaplated', 'superseded'].includes(run.status)) {
      //we don't want to do anything if true
      return
    }

    for (let cycle = 1; cycle <= 24; cycle++) {
      //what are some things we want to do at the top of every loop?
      const current = await(getRun(runId))
      if(['completed', 'failed','cancelled','escaplated', 'superseded'].includes(current.status)) {
        return
      }

      const state = await step.run(`observer-state-${cycle}`, () => {
        return agentState(environmentId, runId)
      })
      // what are things we want to be checkpointed? what are things we don't want to do again if the server shuts down for example
      
      //we don't want this to be in a step because I want this to always be checked everytime it is run
      //note: a lot of async things we do most likely go into a step, and try to only have it do 1 thing in a step
      if (goalSatisfied(state, run.goalCondition)) {
        const report = await step.run(`write-report-${cycle}`, () => {
          return writeReport(run.goal, state)
        })
        await step.run(`complete-run-${cycle}`, ()=> {
          return setRun(runId, 'completed', null, report)
        })
        
        return { report }
      }

      const decision = await step.run(`choose-action-${cycle}`, () => {
        return chooseAction(run.goal, state)
      })

      await step.run(`set-iteration-${cycle}`, () => {
        return setIteration(runId, cycle)
      })

      //record the decision
      await step.run(`record-decision-${cycle}`, () => {
        return recordDecision(environmentId, runId, cycle, decision.action, decision.reason)
      })

      if(decision.action === 'wait' || decision.action === 'request_help' || (decision.action === 'complete' && run.goalCondition)) {
        //force it to keep going and ignore the complete because if it got to this part that means it got past the complete check we have at the beginning of the function
        if (decision.action === 'complete') {
          await step.run(`reject-completion-${cycle}`, () => {
            return logAgentActivity(environmentId, runId, 'human', 'Completion blocked by configured goal condition')
          })
        }

        if(decision.action === 'request_help') {
          await step.run(`defer-help-${cycle}`, () => {
            return logAgentActivity(environmentId, runId, 'human', 'Help is not ready yet', {
              question: decision.detail
            })
          })
        }

        await step.run(`wait-status-${cycle}`, () => {
          return setRun(runId, 'waiting', decision.action === 'request_help' ? 'Help is not added yet' : decision.action === 'wait' ? decision.reason : 'Recovery not added yet')
        })

        let check = 0; //check if we're able to keep going
        //we don't really have to do this but he wanted to do this as a first example
        while (true) {
          const latest = await step.run(`read-event-sequence-${cycle}-${check}`, () => {
            return getRun(runId)
          })
          if(['completed', 'failed','cancelled','escaplated', 'superseded'].includes(current.status)) {
            return
          }

          //indicates that new events have occured since the workflow was last updated
          if(latest.eventSequence > state.eventSequence) {
            break;
          }
          check++
          //we're getting so many events here I want to see what's next, is the idea here
          await step.waitForEvent(`wait-for-service-${cycle}-${check}`, {
            event: 'incident/updated',
            if: `async.data.runId == "${runId}`,
            timeout: '10s',


          })

        }
        continue

      }

      if(decision.reason === 'complete') {
        const report = await step.run(`write-report-${cycle}`, () => {
          return writeReport(run.goal, state)
        })

        await step.run(`complete-run-${cycle}`, () => {
          return setRun(runId, 'completed', null, report)
        })
        return {report}
      }

      // if(decision.action === 'request_help') {
      //   await step.run(`help-unavailable-${cycle}`, ()=>{
      //     return setRun(runId, 'escalated', 'Human help not created yet')
      //   })
      // }

      // doesn't need to be in a step because we using it for logging and recording purposes only,
      //  even though it is not deterministic since it is random
      const actionId = `${runId}:${randomUUID()}`
      const policy = actionPolicy[decision.action]

      if(policy === 'approval') {
        await step.run(`approval-unavailable-${cycle}`, () => {
          return setRun(runId, 'escalated', 'Approval gate is not built yet')
        })
        return
      }

      const result = await step.run(`execute-action-${cycle}`, () => {
        return executeAction(environmentId, runId, actionId, decision.action, policy === 'read' ? undefined : state.world.version)
      })

      if(result.stale === true) {
        await step.run(`stale-action-${cycle}`, () => {
          return logAgentActivity(
            environmentId,
            runId,
            'act',
            'Action rejected because service state changed',
            result
          )
        })

        continue
      }

      await step.run(`settle-status-${cycle}`, () => {
        return setRun(runId, 'waiting', 'Waiting briefly before observing the effect')
      })

      await step.sleep(`settle-${cycle}`, process.env.AGENT_SETTLE_DELAY ?? '1s')
    }

    await step.run('stop-at-limit', () => {
      return setRun(runId, 'escalated', 'Decision limit reached')
    })

    

    // return step.run('whole-agent-loop', async () => {
    //   const run = await getRun(runId)
    //   for (let cycle = 1; cycle <= 8; cycle++) {
    //     const state = await agentState(environmentId, runId)
    //     if (goalSatisfied(state, run.goalCondition)) {
    //       const report = await writeReport(run.goal, state)
    //       await setRun(runId, 'completed', null, report)
    //       return { report }
    //     }
    //     const decision = await chooseAction(run.goal, state)
    //     await setIteration(runId, cycle)
    //     await recordDecision(environmentId, runId, cycle, decision.action, decision.reason)
    //     if (
    //       decision.action === 'wait' ||
    //       decision.action === 'complete' ||
    //       decision.action === 'request_help'
    //     ) {
    //       await setRun(runId, 'escalated', 'This first loop cannot pause yet')
    //       return
    //     }
    //     if (decision.action === 'rollback_release') {
    //       await setRun(runId, 'escalated', 'Approval gate is not built yet')
    //       return
    //     }
    //     const actionId = `${runId}:${randomUUID()}`
    //     const response = await fetch(checkoutServiceUrl + '/operations', {
    //       method: 'POST',
    //       headers: { 'Content-Type': 'application/json' },
    //       body: JSON.stringify({
    //         actionId,
    //         name: decision.action,
    //         expectedVersion: state.world.version,
    //       }),
    //     })
    //     const result = (await response.json()) as Record<string, unknown>
    //     if (!response.ok) throw new Error(String(result.error ?? response.status))
    //     if (result.stale !== true)
    //       await recordToolAction(
    //         environmentId,
    //         runId,
    //         actionId,
    //         decision.action,
    //         { expectedVersion: state.world.version },
    //         result,
    //       )
    //   }
    //   await setRun(runId, 'escalated', 'Decision limit reached')
    // })
  },
)

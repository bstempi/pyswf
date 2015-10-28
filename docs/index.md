# PySwfAws
Finally, a sane way to use SWF and Python

Not to be confused with [pyswf](https://pypi.python.org/pypi/pyswf).

## Purpose
This framework make is easy to build workflows in Python using a "replay" method that's similar to what AWS uses in it's Java and Ruby Flow frameworks.  In short:  You can write workflows that will run locally or remotely without having to worry about most of the workflow details.

## Features
Here's a list of the features that we're the most proud of.

### Ability to Run Locally
Workflows written with this framework can be run in "serial" mode, wich runs the workflow locally in a single thread.  This is useful for testing and debugging.

### Out of Band Data Storage
SWF has limits on how much data you can pack into your responses.  With out of band data storage, you can specify a serializer and data storage class to use during remote execution.  These two components can transparently store serialized data somehwere else, such as S3, and retrieve it later when it is needed.  This allows you to free yourself from some of the SWF service limits.

### Activity Task Retries
Getting throttled by SWF can hurt; if you schedule an activity task, you might later find out that your request was denied and that you have to wait before scheduling more tasks.  When this happens, SWF does *not* store the parameters that were sent to it.  This framework can easily retry tasks and can decide, based on activity's state, wether to retry at all.

### Support for Non-Determenistic and Cached Operations
In order for the replay concept to work, the deciders must be deterministic.  This is difficult if your decider must access a database that changes throughout the workflow.  Also, you don't want your decider re-running queries every time a decision must be made.  With the `@cached` and `@nondeterministic` decorators, the user can call methods and be assured that they only run once.  The result will be transparently stored and retrived on subsequent calls.

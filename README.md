PySWF
=====

Because SWF can be made easier

![Build status](https://travis-ci.org/bstempi/pyswf.svg)
[![Documentation Status](https://readthedocs.org/projects/pyswf/badge/?version=latest)](http://pyswf.readthedocs.org/en/latest/?badge=latest)

Documentation
--------------

[https://pyswf.rtd.org/en/latest](https://pyswf.rtd.org/en/latest)

Purpose
-------

Writing workflows against Amazon's SWF API is really easy.

![Problem, AWS?](http://www.clker.com/cliparts/b/5/d/3/13234313611304932229trollface-md.png)

Actually, it's really hard.  Trying to determine an app's state from a list of events that are defined by SWF can be really difficult.  Mix in some constraints, like the amount of data you can pack into an input or the number of tasks you can schedule, and you will find that you're gonna have a bad time.

That's where PySWF comes in.

PySWF utilizes a replay strategy similar to the ones used by Amazon's Flow framework for Java and Ruby.  By utilizing replay, we can write code that feels and runs synchrounously when executed locally, but runs in a distributed fashion when run with SWF.

State of the Project
---------------------

The code is so alpha that it hurts.  Seriously: none of this is stable.  This is a somewhat-sophisticated proof of concept that needs to be developed much more before it's reliable for some other poor soul besides myself to use it.

Cool Features
--------------
Here's some of the stuff that it currently does:

###Runs Locally
If you wish to run your workflow locally, you may.  PySWF will run your workflow in a serial fashion without SWF, allowing you to easily debug your workflow logic.  Note:  things like serializers or datastores can't be tested like this.

###Activity Retry Support
Because the decider program "replays" every time a decision needs to be made, the arguments that you used to start a task are present, making automatic retry a breeze.  Just set which activity states you're willing to retry and how many times you're willing to fail, and it'll schedule new tasks for you.

###Out of Band Storage
SWF limits how much data you can pack into your fields when making a request.  The largest of them are ~32k.  So, what happens if you need to send an activity task of a workflow a larger input?  With plain SWF, you're out of luck.  In PySWF, you can have each workflow and activity task declare a `Meta` class that contains an instance of a serializer and data storage object they'd like to use.  For instance, you can have an activity task store data as JSON on S3.  This feature is transparent -- the user does't have to write their worker or decider functions to do any serialization.  The interface for adding new serializers and data stores is straight forward, allowing users to use anything, from a database, to a key-value store (like Redis), to something crazy like carrier pidgeons.

###Support for Nondeterministic Operations
Amazon's SWF documentation states that all deciders writted with their Flow framework must be deterministic, otherwise the replay mechanism won't work properly.  But, what happens if your workflow needs to do something nondeterministic, like hit a database and use the results to kick off a child workflow?  In Flow, you can't.  With PySWF, there's a special `@nondeterministic` decorator (`@cached` is an alias).  Functions decorated with one of these will only be run once.  Thier output will be stored out of band and a marker will be inserted into the workflow to allow the framework to find this data later.  On subsequent calls of a `@nondeterministic` function, the framework will retried the out of band data and deserialize it, returning it as if the function were called again.  Not only does this allow for nondeterministic functions, but if can greatly speed up workflow execution for long-running deterministic ones.

###Timer Support
PySWF allows the user to create timers.  The user will receive back a `Promise` that they may probe via `Promise.is_ready()` to see if their timer has fired.  If they wish to block while waiting, they can call `Promise.result()`.  This feature works both locally and via SWF.

Crap I'd Like to do in the Future
---------------------------------

###More Retry Logic
Currently only activity tasks can be retried.  It'd be nice to extend that to timers, signals, and child workflows.

###Signal Support
Signals are pretty important if you're expecting outside parties to inject information into your workflow.  Right now, PySWF has nothing for signals.

###Better Testing
Currently, there are a few unit tests and integration tests.  Because so much of this relies on SWF and S3, I'd like to have a better way of automatically running integration tests against AWS.

###Better Documentation
Yeah, typical dev thing, right?!  I left comments in the code, but it's not enough.  I need more comments, examples, etc.

###Concurrency Within the Decider
Right now, PySWF assumes that the decider is serial.  The first time that someone asks for the value of a promise that's not finished, execution stops.  This may not be desirable for workflows that are managing many things concurrently.  For example, you might have several child workflows running.  Because the first isn't finished, the decider never sees that the second needed attention, causing rescheduling and deciding to be delayed.  This can cause some nasy latency issues.

License
-------
[Apache License](http://directory.fsf.org/wiki/License:Apache2.0)

© 2015, Brian Stempin
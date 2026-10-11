# AI usage

## Tools and setup

- **Agent:** Claude Code, model Claude Opus 5.5.
- **Editor:** GoLand, for running and debugging the programs.
- **Configuration:** `CLAUDE.md`:

```markdown
# How I want to work

- Don't write or change code, or start containers or services, unless I explicitly say so (e.g. "write it", "build it", "go ahead"). Agreeing on a design or direction isn't a go-ahead.
- When I describe a direction, respond with an explanation, a plan or questions, then ask before building anything.
- Keep each change small when I do ask for code.

# Learning

- For code walkthroughs, go function by function for short code and block by block for long functions, and pause between parts for my questions.

# Environment

- Ask me about my coding environment prior to starting.

# Safety

- Don't delete data or recreate databases, brokers or volumes yourself. Give me the commands to run.
- No git commits or pushes without asking.
- Remind me to remove any temporary logging or changes used for easier testing.

# Docs and AI usage

- When a design decision changes, update the design doc and any decisions list in the repo.
- Keep a running note of where I override or correct you.
```
## How I worked

I was mostly new to Go, Docker and Kubernetes, so I used the agent as much for learning as for writing code. The pattern that worked:

1. Ask it to explain a concept or a piece of code, often block by block with pauses for questions.
2. Discuss the options and trade-offs, and make the decision myself.
3. Ask it to lay out a plan, then explicitly tell it to build.
4. Go back through what it wrote until I understood every part.

## Example prompts

- *Ingest:* "tell me about writing a go service to ingest data via a websocket"
- *Broker and scope:* "let's go with kafka since it's the industry standard. let's start with a stateless version and the worker will just print an event when it gets it. let's also not worry about dropped events."
- *Keyword matching:* "let's start with post keyword matching filtering on English and the word hockey. do whole word and contains word"
- *Burst detection:* "let's test with 2 follows per 10 minutes but in production move to 50 follows in 5 minutes. let's discuss the windowed counter algo first too", followed by "how would the queue and map work if someone clicked follow and unfollow a bunch of times in a row? how does the queue know when to remove someone?"

## Where it helped

- Explaining Go, Kafka, and Kubernetes concepts and answering questions. 
- Laying out options with trade-offs for each decision (window types, partition keys, DLQ placement, startup ordering).
- Correcting itself when I pushed back: after a question, it found that an earlier claim was incorrect, and checked library behavior against the source instead of guessing.

## Where I overrode it or changed its approach
- **Cursor resume:** it rewound 5 seconds on reconnect. I removed the rewind once we established the client always reconnects to the same Jetstream instance, so the exact cursor misses nothing.
- **Init container:** it proposed waiting for Kafka indefinitely; I asked for a 5-minute limit so a stuck startup becomes a visible failure.
- **Memory maintenance:** I suggested a timed sweep at a low traffic time of day. We kept the frequent event-time sweep instead because the stream is global and there might not be a low traffic time.
- **Kafka outage behavior:** its failure-test instructions and DESIGN.md said that with Kafka down, the router's memory would climb until it was OOM-killed. When I ran the test, memory stayed flat. Prompted by that, it read the kafka-go source and found that an unreachable broker isn't a retried error, so each batch is dropped on its first attempt; memory only builds up when Kafka is slow, not gone. Its first explanation of my result was also wrong (it said writes would fail immediately, which missed the client's metadata cache), so I had it check the source again before changing the docs. The same review found the router logged one error per failed batch, flooding the log during an outage; I had it count drops and report them in the 5-second summary instead.

# Exercise 12 — CEP (Complex Event Processing)

## What You'll Learn
- Pattern API — define sequences of events to match
- begin/next — strict contiguity between events
- PatternProcessFunction — process matched event sequences
- Keyed patterns — match patterns within each key group

## Core Concepts
```java
Pattern<Event, ?> pattern = Pattern.<Event>begin("first")
    .where(e -> "LOGIN".equals(e.action()))
    .next("second")
    .where(e -> "LOGIN".equals(e.action()));
PatternStream<Event> ps = CEP.pattern(stream.keyBy(Event::userId), pattern);
```

## Gotchas
- Patterns are evaluated per key — use keyBy() before CEP.pattern()
- For time-bounded patterns, add .within(Time.minutes(5))

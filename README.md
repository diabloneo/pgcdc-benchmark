# pgcdc-benchmark
Benchmark tools for Postgres CDC application.

# Design

## Source

Generating CDC events through SQL commands.

## Sink

Simulating CDC events consumers. Every consumer will subscribe to all tables and consume all events.

Each simulated consumer is a single goroutine, it will consume evnets at same order they produced.

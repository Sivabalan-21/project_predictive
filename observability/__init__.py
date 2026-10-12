"""Prometheus metrics for the API and the consumer.

SINGLE-PROCESS ASSUMPTION: every process keeps its own in-memory registry. That is correct for one uvicorn worker and one
consumer process (the shipped Docker setup). With several API workers each scrape would reach one random worker and the
counters would look inconsistent - see docs/observability.md for the changes multiprocess deployment needs.
"""

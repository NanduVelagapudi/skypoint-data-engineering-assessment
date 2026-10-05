"""Pure field parsers for cleaning raw values (Task 2).

Every parser takes one raw text value plus the source system's conventions as
arguments, and returns a ParseResult. Parsers never read files, write to the
database, log, or raise on bad input. They return reason codes only; mapping a
reason code to a severity (error or warning) is Task 6's job.
"""

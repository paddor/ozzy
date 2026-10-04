# ozzy-io

Backend-neutral asynchronous file operations for Ozzy. Submission lanes enforce
count and byte bounds, with separate capacity for progress work.

Admitted operations retain their buffers and handles until completion, including
when an observer is dropped. Journal owners decide which completed bytes form a
durable prefix; backends own physical execution.

# Google Pregel Implemetation

This is a dummy implementation of Google's Pregel framework in Python as a part of the Cloud Computing (COL733) Course Project at IIT Delhi. It runs on a single machine, where different processes emulate various workers.

We have used `REDIS` for orchestrating message transfers during the communication step.

Contributed by:

1. Shivam Verma (2020CS50442)
2. Geetansh Juneja (2020CS50649)
3. Hemank Bajaj (2020CS10349)

The original paper can be found: [Here](https://dl.acm.org/doi/10.1145/1807167.1807184)

The detailed report for our project can be found: [Here](https://github.com/Shivam5022/Google-Pregel-Implemetation/blob/main/report/report.md)
**Tests** folder contains 2 test programs: PageRank Computation and MaxNode in Graph.

#### Steps to run PageRank Example:

1. Start the Redis server (`brew services start redis` in Mac)
2. Run `python3 pgrank.py` 

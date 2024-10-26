# Spark_Optimization

Original: Counts the number of questions grouped by question_id and month

![image](https://github.com/user-attachments/assets/2fd3fdb3-c7b4-45b1-97ee-c50a41527f62)

From the Spark UI at localhost:4040 we can examine the individual jobs and see runtime duration. Total time of all jobs is 4.4s.

![image](https://github.com/user-attachments/assets/9bd3b2aa-860f-47b8-88e4-d82b261314a3)
![image](https://github.com/user-attachments/assets/87af6e8a-6570-440c-a013-987714b235d1)

Dag for each job:
![image](https://github.com/user-attachments/assets/225d404d-20d2-492b-b662-64121383fa92)

Running .explain() on resultDF shows the Physical Plan: 

![image](https://github.com/user-attachments/assets/8eaa8dfc-2ff2-45d4-b4e4-3b4deffaac8e)

## Code refactoring to improve run speed:
**- Caching:**
A common method of improving performance is persisting data into cache so it can be reused. Here I cache answers_month.

![image](https://github.com/user-attachments/assets/ec43cff6-74fb-4847-a6ce-557e065e59b9)

**- Result – Caching:**

![image](https://github.com/user-attachments/assets/f740df04-52ef-4a10-bc22-3e62596f1c31)
![image](https://github.com/user-attachments/assets/4cee8862-e7b3-4067-828a-ecfc99b49ee9)

The final step (Job ID 5) decreased from 0.8s to 0.5s, but the additional step of caching the data increased the overall time (5s for Job ID 3). Total time of all jobs is 10.4s.

**- Repartition:**
Repartitioning divides the data and helps prevent skew. Here I repartition answers_month into 10 partitions by question_id.

![image](https://github.com/user-attachments/assets/d647ac16-d741-45b7-a4a9-45dd48df6a4b)

**- Result - Repartition:**

![image](https://github.com/user-attachments/assets/fc5fa41a-8175-4d40-a55e-b8e65c313f8e)
![image](https://github.com/user-attachments/assets/2821bb6a-7bc7-4aea-ba91-b86afbb91ccf)

The job is small, so we still do not see much improvement. This time, the final stage has decreased from 0.8s to 0.3s, however, the timing for jobs in the middle have slightly increased. Total time of all jobs is 5s.

## Conclusion:
Both repartitioning and caching improved the performance of the final step but had too high of an overhead cost to be beneficial overall. However, if the dataset was larger and the last step took a larger percentage of the total run time, then both these methods would work well in reducing the time spent.  










# Big-Data
Coursework for Big Data Processing




ASSIGNMENT
Write a set of Map/Reduce (or Spark) jobs that process the given input and generate the data required to answer the following questions:

PART A. TIME ANALYSIS (20%)
Create a bar plot showing the number of transactions occurring every month between the start and end of the dataset.

Create a bar plot showing the average value of transaction in each month between the start and end of the dataset.

Note: As the dataset spans multiple years and you are aggregating together all transactions in the same month, make sure to include the year in your analysis.

Note: Once the raw results have been processed within Hadoop/Spark you may create your bar plot in any software of your choice (excel, python, R, etc.)

PART B. TOP TEN MOST POPULAR SERVICES (20%)
Evaluate the top 10 smart contracts by total Ether received. An outline of the subtasks required to extract this information is provided below, focusing on a MRJob based approach. This is, however, only one possibility, with several other viable ways of completing this assignment.

JOB 1 - INITIAL AGGREGATION
To workout which services are the most popular, you will first have to aggregate transactions to see how much each address within the user space has been involved in. You will want to aggregate value for addresses in the to_address field. This will be similar to the wordcount that we saw in Lab 1 and Lab 2.

JOB 2 - JOINING TRANSACTIONS/CONTRACTS AND FILTERING
Once you have obtained this aggregate of the transactions, the next step is to perform a repartition join between this aggregate and contracts (example here). You will want to join the to_address field from the output of Job 1 with the address field of contracts

Secondly, in the reducer, if the address for a given aggregate from Job 1 was not present within contracts this should be filtered out as it is a user address and not a smart contract.

JOB 3 - TOP TEN
Finally, the third job will take as input the now filtered address aggregates and sort these via a top ten reducer, utilising what you have learned from lab 4.

PART C. TOP TEN MOST ACTIVE MINERS (10%)
Evaluate the top 10 miners by the size of the blocks mined. This is simpler as it does not require a join. You will first have to aggregate blocks to see how much each miner has been involved in. You will want to aggregate size for addresses in the miner field. This will be similar to the wordcount that we saw in Lab 1 and Lab 2. You can add each value from the reducer to a list and then sort the list to obtain the most active miners.

PART D. DATA EXPLORATION (50%)
The final part of the coursework requires you to explore the data and perform some analysis of your choosing. These tasks may be completed in either MRJob or Spark, and you may make use of Spark libraries such as MLlib (for machine learning) and GraphX for graphy analysis. Below are some suggested ideas for analysis which could be undertaken, along with an expected grade for completing it to a good standard. You may attempt several of these tasks or undertake your own. However, it is recommended to discuss ideas with Joseph before commencing with them.

SCAM ANALYSIS
Popular Scams: Utilising the provided scam dataset, what is the most lucrative form of scam? How does this change throughout time, and does this correlate with certain known scams going offline/inactive? (15/50)
MACHINE LEARNING
Price Forecasting: Find a dataset online for the price of ethereum from its inception till now. Utilising Spark mllib build a price forecasting model trained on this, the coursework dataset and any other useful information sources you can find. How accurate can you get your forecast within the coursework window to June 2019? How far past June 2019 does your forecast remain accurate? (20-25/50)
GRAPH ANALYSIS
For these challenges you can use Spark GraphX to build the transactions dataset into a directed graph where the from and to addresses are the vertices and each transaction is an edge. Label the edges with the value of each transaction and the time of transaction.

Triangle Count/Clustering coefficient: Triangle Count is a community detection graph algorithm that is used to determine the number of triangles passing through each node in the graph. A triangle is a set of three nodes, where each node has a relationship to all other nodes. Write your own triangle count algorithm using GraphX's pregel API and report your findings (20/50). You can expand upon this to calculate local and global clustering coefficients (25/50)

Scammer Graph Traversal: Once Ether have been scammed what happens to it afterwards? Utilising GraphX's Pregel API implement a graph traversal algorithm to find addresses where scammers are accumulating ether. It is common for stolen crypto to be tumbled, can your algorithm mitigate against this? From your results can you identify several separate scams in the dataset which appear to be the same bad actor/group? This is a hard challenge (25/50)

MISCELLANEOUS ANALYSIS
Fork the Chain: There have been several forks of Ethereum in the past. Identify one or more of these and see what effect it had on price and general usage. For example, did a price surge/plummet occur and who profited most from this? (10/50)

Gas Guzzlers: For any transaction on Ethereum a user must supply gas. How has gas price changed over time? Have contracts become more complicated, requiring more gas, or less so? How does this correlate with your results seen within Part B. (10/50)

Comparative Evaluation Reimplement Part B in Spark (if your original was MRJob, or vice versa). How does it run in comparison? Keep in mind that to get representative results you will have to run the job multiple times, and report median/average results. Can you explain the reason for these results? What framework seems more appropriate for this task? (10/50)

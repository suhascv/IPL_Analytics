# Big Data Analytics - Group Project
## IPL dataset
### Authors:
* Suhas CV
* Mihir Manjrekar
* Gautam Gadipudi

The Raw Data of the project can be found in Data directory.

The Source code is found src directory.

The following external python libraries were used 
* [glob](https://pypi.org/project/glob3/)
* [pymongo](https://pypi.org/project/pymongo/)
* [yaml](https://pypi.org/project/PyYAML/)

Installation:
execute the codes in the following order to have dataset installed on your local Mongo DB server:
```
python3 src/load_player.py
```
```
python3 src/load_matches.py
```
```
python3 src/update_players.py
```
```
python3 src/get_partnerships.py
```
```
python3 src/extract_score.py
```
```
python3 src/runs_per_match.py
```

## Analysis:
### Clustering:
We are going to cluster based on the following attributes per match / game:
* `runs`
* `strike_rate`
* `sixes`
* `fours`

Firstly, normalize the data. Run the below script to get the normalized data into a collection named ***clustering_data***:
```
python3 src/normalize_batting_stats.py
```

Then, run the following to get a plot of ***k*** (number of clusters) vs ***SSE*** (Sum of Squared Errors):
```
python3 src/Clustering/plot_sse_k.py
```

Then, select a particular k at the elbow of the plot (saved under `./Visualizations/Clustering`) and run the following:
```
python3 src/analysis/Clustering/cluster.py <k-value> <iteration-limit>
```


### Association:
 We are going to perform item set mining (apriori) on batting partnerships which has following attributes:
 * `partners(partner-1 ,partner-2)`
 * `venue`
 * `total runs`
 
 Initially, we will filter the partnerships with total runs > 30. The algorithm works sligthly different from usual apriori, here we have fixed number of items in a partnership(transaction), thus the algorithm stops at level 3. The minimum support is set to 15.
* At Level 1 we will get the the individual players/venues involved in atleast 15 partnerships(30+ run).
* At Level 2 we will have the player-venue/player1-player2 involved in atleast 15 partnerships(30+ run).
* At Level 3 we will have the most frequent partnerships(partner1,partner2,venue).

```
python3 src/analysis/Association/partnershipVenueMining.py
```
### PeerEval:
 ## Mihir MAnjrekar:
  * Mining Analysis
  * Visulization of Attributes and their analysis
  * small part in pre processing(mainly data cleaning, getting rid of the missing values)
  * Apriori implementation with suhas
  * some part in feature analysis( like which feature to select and what feature can be used for visualization)
  * Implemented extract_score.py, get_partnership.py, runs_per_match.py
 ## Suhas Chikkanaravangala Vijayakumar:
  * Mining analysis and Apriori implementation with mihir
  * Analyzied the dataset for outliers and missing values
  * I took most of the pre-processing task that we listed during our weekly meetings
  * Did cluster analysis with Gautuam, also implemented the visualization of the clusters and the ranking system.
  * Also Implemented load_players.py, ,load_matches.py, update_player.py
 ## Gautam Gadipudi
  * Cluster Analysis with suhas, also the task to implemented k-means.
  * Implemeted normalize_batting_stats.py, plot_sse_k.py,cluster.py
  * Visulizations of best K value.

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
python3 src/update_players.py
```
```
python3 src/get_partnerships.py
```
```
python3 src/extract_score.py
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
python3 src/Clustering/cluster.py <k-value> <iteration-limit>
```

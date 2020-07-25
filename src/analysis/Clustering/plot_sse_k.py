import matplotlib.pyplot as plot
from cluster import do, get_groups_points

def get_SSE() -> float:
    SSE = 0
    centroid_groups = get_groups_points()
    for centroid in centroid_groups:
        point = centroid['point']
        for obj in centroid['data_objects']:
            SSE += (obj['normalizedData'][0] - point[0]) ** 2
            SSE += (obj['normalizedData'][1] - point[1]) ** 2
            SSE += (obj['normalizedData'][2] - point[2]) ** 2
            SSE += (obj['normalizedData'][3] - point[3]) ** 2

    return SSE

def plot_graph(k_SSE_dic: dict):
    plot.title('k vs SSE')
    k_list = []
    SSE_list = []
    for k in k_SSE_dic:
        k_list.append(k)
        SSE_list.append(k_SSE_dic[k])
    plot.plot(k_list, SSE_list)
    plot.xlabel('k value')
    plot.ylabel('Sum of squared error')
    plot.savefig(f'Visualizations/Clustering/SSE_k_plot3.png', format='png')
    plot.clf()

def main():
    k_SSE_dic = {}
    for k in range(2, 20, 2):
        do(k, iter=50)
        SSE = get_SSE()
        k_SSE_dic[k] = SSE        
    plot_graph(k_SSE_dic)

if __name__ == "__main__":
    main()
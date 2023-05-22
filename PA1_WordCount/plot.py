# draw plot of the data
import matplotlib.pyplot as plt
import numpy as np
from scipy.interpolate import interp1d

shard = np.array([10, 20, 50, 100, 200, 400, 500, 750, 1000])
k_5 = [320.4804780483246, 315.54278683662415, 303.5954396724701, 289.0423038005829, 280.0456397533417, 267.2499969005585, 330.9343419075012, 290.5871479511261]
k_10 = [308.9026589393616, 299.8877942562103, 279.9842629432678, 270.81034111976624, 273.1676616668701, 274.32711601257324, 331.6158242225647, 313.7171621322632]
k_15 = [310.3715901374817, 307.3906319141388, 309.8442847728729, 284.3680899143219, 289.9586548805237, 298.03590512275696, 295.7874221801758, 268.9401898384094]
k_20 = [309.237832069397, 302.0413417816162, 278.2356688976288, 263.4939172267914,  227.66605687141418,  251.84302806854248, 294.58049511909485, 260.7667980194092]

avg_st = np.array([0.38020344845555054, 0.25093529273370463, 0.1130081526324426, 0.06191158059398134, 0.018463491804380835, 0.013296684715578917, 0.010763277173401432, 0.007559138235643859, 0.0038448973720571727])

def plotTimebyK():
    plt.plot(shard, k_5, 'y-o')
    plt.plot(shard, k_10, 'r-o')
    plt.plot(shard, k_15, 'b-o')
    plt.plot(shard, k_20, 'g-o')
    plt.legend(['k=5', 'k=10', 'k=15', 'k=20'], loc='upper right')
    plt.title('data_2.5GB: Time taken by different numbers of shards')
    plt.xlabel('shard')
    plt.ylabel('time taken (s)')
    plt.yticks(range(250, 350, 25))
    plt.xticks(range(0, 5000, 500))
    plt.show()

def plotAverageShardTimeByShard():
    cubic = interp1d(shard, avg_st, kind='cubic')
    xs = np.linspace(10, 1000, 10000)
    ys = cubic(xs)

    plt.plot(shard, avg_st, 'g-o')
    plt.title("Top K Words: Average Shard Processing Time vs Shard Count\n for 300 MB Test File")
    plt.ylabel("Average Shard Processing Time(Seconds)")
    plt.xlabel("Shard Count")
    # plt.xticks(range(0,1000,50))
    plt.legend(["K = 10"])
    # plt.yscale("log")
    
    # plt.savefig("AverageShardTimeByShardCount.png")
    plt.show()


if __name__ == '__main__':
    plotAverageShardTimeByShard()

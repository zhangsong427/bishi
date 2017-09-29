from SparkContext import sc
from pyspark.mllib.clustering.KMeansModel import KMeans

def main():
    data = sc.textFile('Kmeans_Data.txt')
    k = 4 #假设总共聚成4类

    model = KMeans.train( data, k, maxIterations = 60, runs = 5, initializationMode = 'random') #模型并行训练

if __name__ == '__main__':
    main()
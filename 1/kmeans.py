from numpy import *
from matplotlib import pyplot as plt


#加载数据
def loadDataSet(fileName):
    dataMat = []
    fr = open(fileName)
    for line in fr.readlines():
        curLine = line.strip().split('\t')
    fltLine = map(float, curLine)
    dataMat.append(fltLine)
    return dataMat

#计算两个向量的距离，用的是欧几里得距离
def distEclud(vecA, vecB):
    return sqrt(sum(power(vecA - vecB, 2)))

#随机生成初始的质心
def randCent(dataSet, k):
    m, dim = dataSet.shape  #数据维度为dim(N)
    centroids = zeros((k, dim))
    for i in range(k):
        index = int(random.uniform(0, m))
        centroids[i, :] = dataSet[index, :]
    return centroids

#定义Kmeans函数
def kMeans(dataSet, k, distMeas=distEclud, createCent=randCent):
    m, dim = dataSet.shape
    clusterAssment = mat(zeros((m,dim)))
    centroids = createCent(dataSet, k)
    clusterChanged = True
    while clusterChanged:
        clusterChanged = False
        for i in range(m):
            minDist = inf
            minIndex = -1
            for j in range(k):
                distJI = distMeas(centroids[j,:],dataSet[i,:])
                if distJI < minDist:
                    minDist = distJI; minIndex = j
            if clusterAssment[i,0] != minIndex:
                clusterChanged = True
            clusterAssment[i,:] = minIndex,minDist**2
        print (centroids)
        for cent in range(k):
            ptsInClust = dataSet[nonzero(clusterAssment[:,0].A==cent)[0]]
            centroids[cent,:] = mean(ptsInClust, axis=0)
    return centroids, clusterAssment

#将聚类结果显示
def show(dataSet, k, centroids, clusterAssment):
    m, dim = dataSet.shape
    if dim != 2:
        print ('Sorry! I can not draw because the dimension of your data is not 2!')  #不能显示高于2维的聚类结果
        return 1
    mark = ['or', 'ob', 'og', 'ok', '^r', '+r', 'sr', 'dr', '<r', 'pr']  #结果点显示设置
    if k > len(mark):
        print ('Sorry! Your k is too large! please contact Zouxy')  #质心数太多不能显示
        return 1

    for i in range(m):
        markIndex = int(clusterAssment[i, 0])
        plt.plot(dataSet[i, 0], dataSet[i, 1], mark[markIndex])   #选择两个维度进行显示聚类结果

    mark = ['Dr', 'Db', 'Dg', 'Dk', '^b', '+b', 'sb', 'db', '<b', 'pb']   #结果质心显示设置
    for i in range(k):
        plt.plot(centroids[i, 0], centroids[i, 1], mark[i], markersize = 10)  #选择两个维度进行显示分类结果质心
    plt.show()

def main():
    dataMat = mat(loadDataSet('Kmeans_Data.txt'))
    k = 4
    myCentroids, clustAssing= kMeans(dataMat,k) #假设总共聚成4类
    print (myCentroids)
    show(dataMat, k, myCentroids, clustAssing)


if __name__ == '__main__':
    main()
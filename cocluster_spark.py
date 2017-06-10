# Databricks notebook source
import random
import itertools
from pyspark.sql import SQLContext
import math

def Mapper(val):
  codebook = B.value 
  element_a = val[0][0] #User or Item
  element_b_ratings = val[0][1] # [(ItemId1, Rating), ...] for User. or [(UserId1, Rating), ...] for Item
  type_element = val[1] #row or col

  if type_element == 'row':
    a_cluster_index = U.value
    b_cluster_index = V.value 
  else: 
    a_cluster_index = V.value
    b_cluster_index = U.value 
 
  g_i = Statistics(element_b_ratings, b_cluster_index) 
  min_error = float('inf')
  best_cluster = a_cluster_index[element_a]  
  for a_cluster in set(a_cluster_index.values()):
    current_error = 0.0
    for element, rating in element_b_ratings:
      b_cluster = b_cluster_index[element]
      if type_element == 'row':
        current_error += math.pow((rating - codebook[(a_cluster,b_cluster)] ), 2)
      else:
        current_error += math.pow((rating - codebook[(b_cluster,a_cluster)] ), 2)
    
    if len(element_b_ratings) > 0 and (current_error < min_error) and not math.isnan(current_error):
      min_error = current_error
      best_cluster = a_cluster
      a_cluster_index[element_a] = best_cluster

  #returns the best [User/Item]cluster assignment for the given "a", and ratings for each b [Item/User]cluster related to "a", as well as all the element "a" itself
  return (best_cluster,(best_cluster, (g_i, {element_a})))
  
def Statistics(element_ratings, element_cluster_index):
  g = {}
  for element, rating in element_ratings:
    cluster_index = element_cluster_index[element]
    if cluster_index in g:
      g[cluster_index].append(rating)
    else:
      g[cluster_index] = [rating]
  return g

def Reducer(k, v):
  print 'k --> '+str(k)
  cluster = k[0]
  rows = [(k[1][0],k[1][1]), (v[1][0],v[1][1])]
  g_p = {} #g_p: Contains for each itemCLuster/userCluster the total ratings of all users/items belonging to this userCluster/itemCluster
  I_p = set()   #I_p: all users/items belonging to the Cluster 
  for g, I in rows:
    g_p = CombineStatistics(g_p, g)
    I_p = I_p.union(I)
  return (cluster, [g_p, I_p])


def CollectResults(values, element_type):
  G = B.value
  cluster_id = values[0]
  g_p = values[1][0]
  Ip = values[1][1]
  
  if element_type == 'row':
    cluster_a = U.value
  else:
    cluster_a = V.value
  
  for element in Ip:
    cluster_a[element] = cluster_id
    for cluster_b, ratings in g_p.iteritems():
      if len(ratings) > 0 and sum(ratings) > 0:
        avg = sum(ratings)/len(ratings)
      else:
        avg = float('inf')
        
      if element_type == 'row':
          G[(cluster_id,cluster_b)] = avg
      else:
        G[(cluster_b,cluster_id)] = avg
  return cluster_a, G 
 
#returns the sum of ratings for the each a_cluster given the b cluster
def CombineStatistics(g_p, g):
  for cluster, ratings in g.iteritems():
    if cluster not in g_p:
      g_p[cluster] = ratings
    else:
      g_p[cluster] += ratings
  return g_p
 
T = 10
L = K = 20
#Full data
raw_data = sc.textFile("/FileStore/tables/a4xcme5w1482155395512/")

#Sample data
#raw_data = sc.textFile("/FileStore/tables/mr89bn6r1483783271514/ratings_small.txt")

parsed_data = raw_data.map(lambda x: x.split(",")).\
                      map(lambda x: (random.randrange(0,K-1,1), random.randrange(0, L-1,1), int(x[0]), int(x[1]),int(x[2])))
  
users_df = sqlContext.createDataFrame(parsed_data,(['userCluster','itemCluster','user','item','rating']))

U = {val[2]:val[0] for val in users_df.rdd.collect()}
V = {val[3]:val[1] for val in users_df.rdd.collect()}

users_df.registerTempTable("users")
averages_by_cluster = sqlContext.sql("SELECT userCluster, itemCluster, avg(rating) FROM users group by userCluster, itemCluster")

B = dict.fromkeys(list(itertools.product(range(K), range(L))), 0.0)
for val in averages_by_cluster.rdd.collect():
  B[(val[0],val[1])] = val[2]

V = sc.broadcast(V)
U = sc.broadcast(U)
B = sc.broadcast(B)

users_rdd = users_df.rdd.map(lambda x: (x[2], [(x[3], x[4])]))\
                        .reduceByKey(lambda x,y: x+y)\
                        .map(lambda x :  (x,'row'))

items_rdd = users_df.rdd.map(lambda x: (x[3], [(x[2], x[4])]))\
                          .reduceByKey(lambda x,y: x+y)\
                          .map(lambda x : (x,'col'))

t=1
while t <= T:
  users_result = users_rdd.map(Mapper)
  users_result = users_result.reduceByKey(Reducer).collect()
  for i,j in users_result:
    U, B = CollectResults(j,'row')
    B = sc.broadcast(B)
    U = sc.broadcast(U)

  items_result = items_rdd.map(Mapper).reduceByKey(Reducer).collect()
  for i,j in items_result:
    V, B = CollectResults(j,'col')
    B = sc.broadcast(B)
    V = sc.broadcast(V)
  
  t += 1

df = sc.parallelize([(k[0],k[1],v) for k, v in B.value.items()])
df = sqlContext.createDataFrame(df, ['UserCluster','ItemCluster','Rating']).saveAsTable("Codebook", mode='overwrite')

df = sc.parallelize([(k,v) for k, v in U.value.items()])
df = sqlContext.createDataFrame(df, ['UserId','UserCluster']).saveAsTable("U", mode='overwrite')

df = sc.parallelize([(k,v) for k, v in V.value.items()])
df = sqlContext.createDataFrame(df, ['ItemId','ItemCluster']).saveAsTable("V", mode='overwrite')



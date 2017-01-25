prod = sc.textFile("/user/hive/warehouse/products")
prodmap = prod.map(lambda x : (int(x.split(',')[1]), x))
def getTopN(rec, num):
    x = []
    x = list(sorted(rec[1], key = lambda k : float(k.split(',')[4]), reverse=True))
    import itertools
    return (y for y in list(itertools.islice(x, 0, num)))

prodmap = prod.map(lambda x : (int(x.split(',')[1]), x))
for i in prodmap.groupByKey().flatMap(lambda x: getTopN(x,3)).collect(): print(i)

##--------------------------------------
def getTopDenseN(rec, topN):
  x = [ ]
  topNPrices = [ ]
  prodPrices = [ ]
  prodPricesDesc = [ ]
  for i in rec[1]:
    prodPrices.append(float(i.split(",")[4]))
  prodPricesDesc = list(sorted(set(prodPrices), reverse=True))
  import itertools
  topNPrices = list(itertools.islice(prodPricesDesc, 0, topN))
  for j in sorted(rec[1], key=lambda k: float(k.split(",")[4]), reverse=True):
    if(float(j.split(",")[4]) in topNPrices):
      x.append(j)
  return (y for y in x)
  
for i in prodmap.groupByKey().flatMap(lambda x: getTopDenseN(x,3)).collect(): print(i)

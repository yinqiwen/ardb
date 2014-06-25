# 2D Spatial Index #
Ardb add 2d spatial data index support in v0.7.0. This document explain the design details.   
Generally, this design can be simplely described as 'GeoHash + Sorted Set'. It's easy to port this solution to redis. 
 
## Internals ##
### Spatial Data ###
2d spatial point can be described as a tuple like (latitude, longtitude, value). The point would be stored as an element in a sorted set.
### GeoHash Integer ###
[Geohash](http://en.wikipedia.org/wiki/Geohash) is a latitude/longitude geocode system which could encode latitude/longtitude with a precision into several bits. More precision would make geohash result more bits.  
Since sorted set only accept number as the score value. We need a way to convert the geohash result to a number value.  
I wrote another C99 library [geohash-int](https://github.com/yinqiwen/geohash-int) to encode latitude/longitude to a 64bit integer.  (Almost all geohash libray listed in [Geohash wiki](http://en.wikipedia.org/wiki/Geohash) only give a base32 string result. That's why i write that library).  

### Store Spatial Data ###
Only two steps:  
1. Use [geohash-int](https://github.com/yinqiwen/geohash-int) to encode latitude/longitude with 26 steps which would generate a 52bit integer value. This result would have a distance precision about 0.6 meters.  
2. Use the 52bit geohash integer as the score value, and use 'ZADD' command to store the spatial data.
   
    ZADD key <GeoHash50Bits>  <value>
### Search  ###
The search condition is a given coordinate and radius. For example, seach all points within a 1000m radius of longitude/latitude coordinate (120.0, 25.0).  

First of all, We should estimate the geohash encoding bits by raius value first. Since geohash value represent a box, considering the edge case refered in [Geohash Wiki](http://en.wikipedia.org/wiki/Geohash), to serach fast, we need find the smallest geohash box with surrounding 8 geohash box that could cover all points in radius in the worst case. This is a simple table about the estimate geohash encoding bits with radius.
    
    HashBits, Radius Meters
    52, 0.5971
    50, 1.1943
    58, 2.3889
    46, 4.7774
    44, 9.5547
    42, 19.1095
    40, 38.2189
    38, 76.4378
    36, 152.8757
    34, 305.751
    32, 611.5028
    30, 1223.0056
    28, 2446.0112
    26, 4892.0224 
    24, 9784.0449
    22, 19568.0898
    20, 39136.1797
    18, 78272.35938
    16, 156544.7188
    14, 313089.4375
    12, 626178.875
    10, 1252357.75
    8, 2504715.5
    6, 5009431
    4, 10018863

1. Encode longitude/latitude coordinate with estimate bits by [geohash-int](https://github.com/yinqiwen/geohash-int).  
2. Find surrounding 8 neighbors' geohash integer value by [geohash-int](https://github.com/yinqiwen/geohash-int).
3. For each geohash integer value, we generate a pair (GeoHashIneger, GeoHashIneger + 1). Then we got 9 pairs.  
4. For each pair, we convert it to a score range. Any integer value in the pair should be left shift to 52 bits. The shifted value is the smallest 52 bits geohash value in the geohash box represent by unshifted GeoHashInteger.    
   For example, if we need search points in radius 3000m, then we should encode the  longitude/latitude coordinate to a integer with 26 bits, then left shift it 26 bits, then we got a 52 bits integer.
5. For each score range, use 'ZRANGEBYSCORE key min max WITHSCORES' to retrive all point's value and it's score.
6. For each point value and it's score, we can decode the score to a GeoHash area by [geohash-int](https://github.com/yinqiwen/geohash-int) and compute the distance with given longitude/latitude , then compare the distance with given radius value to exclude the point not in radius.


### Extended Commands ###
----------

#### GeoAdd ####
Syntax: **GEOADD key [WGS84|MERCATOR] longitude latitude value**  

#### GeoSearch ####
Syntax: **GEOSEARCH key LOCATION lat lon RADIUS r [ASC|DESC] [WITHCOORDINATES] [WITHDISTANCES] [GET pattern [GET pattern ...]] [LIMIT offset count]**  

### Performance
27,000,000 spatial points with mercator coordinates saved in a sorted set.  
Four-core Intel(R) Xeon(R) CPU E5520 @2.27GHz, with 16 GB RAM.  
Ardb: 4 threads    
All data cached in memory, about 11000 qps to search with radius 1000m, 150 points matching, 100 points matched.  
All data in leveldb without cache, about 900 qps to search with radius 1000m, 150 points matching, 100 points matched. 


## Zookeeper Path
    
    /<cluster_name>/config               --- store all cluster config
    /<cluster_name>/servers/<server>     --- EPHEMERAL path created by member
    /<cluster_name>/partitions<gid>  --- Cluster partitions info
    /<cluster_name>/nodes/<nid>      --- Cluster node info
    /<cluster_name>/slots/<sid>      --- Cluster node info

## Zookeeper Data Structure
All data is json encoded.
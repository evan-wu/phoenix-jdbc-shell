An interactive phoenix shell using jdbc connection.

# Purpose

Phoenix sqlline shell depends hbase configuration - hbase-site.xml, what's different from phoenix sqlline shell is use pure jdbc connectivity to verify queries executed in jdbc environment. 

# Phoenix version
```
<dependency>
    <groupId>org.apache.phoenix</groupId>
    <artifactId>phoenix-core</artifactId>
    <version>4.6.0-HBase-1.1</version>
</dependency>
```

# Usage
* mvn clean install
* edit config.properties
* command mode: java -jar phoenix-shell-1.0-SNAPSHOT.jar <SQL statement>
* interactive mode: java -jar phoenix-shell-1.0-SNAPSHOT.jar